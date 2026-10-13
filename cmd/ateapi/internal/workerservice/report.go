// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workerservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ReportActorSnapshotState records the snapshot state an atelet reports for an
// Actor on its node.
func (s *Server) ReportActorSnapshotState(ctx context.Context, req *ateapipb.ReportActorSnapshotStateRequest) (*ateapipb.ReportActorSnapshotStateResponse, error) {
	// Authz never checks WorkerService, so this is the only gate. It runs
	// before anything reads the request or the store.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateReportActorSnapshotStateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())

	// Use authoritative state to authorize the report, never the request
	// itself. Every refusal is NotFound, so an atelet learns nothing about
	// Actors on other nodes.
	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while fetching actor %s: %w", actorRef, err)
	}
	if actor.GetMetadata().GetUid() != req.GetActorUid() || !actorOnNode(actor, caller.NodeName) {
		slog.WarnContext(ctx, "Refusing a snapshot state report for an actor not on the caller's node",
			slog.String("actor", actorRef.String()),
			slog.String("requested_uid", req.GetActorUid()),
			slog.String("caller_node", caller.NodeName))
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	}

	if err := s.control.ApplyActorSnapshotState(ctx, actorRef, req.GetActorUid(), caller.NodeName, req.GetState()); err != nil {
		return nil, err
	}
	return &ateapipb.ReportActorSnapshotStateResponse{}, nil
}

// actorOnNode reports whether the Actor is placed on node, or holds a local
// snapshot there.
func actorOnNode(actor *ateapipb.Actor, node string) bool {
	if actor.GetStatus().GetAssignedNode() == node {
		return true
	}
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetLocality() == node {
			return true
		}
	}
	return false
}
