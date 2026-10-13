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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// seedPausedActor stores a PAUSED actor whose local snapshot is on node.
func seedPausedActor(t *testing.T, st store.Interface, node string) *ateapipb.Actor {
	t.Helper()
	return storetest.MustCreateActor(t, context.Background(), st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: suspendAtespace, Name: suspendActor},
		Status: &ateapipb.ActorStatus{
			State:        ateapipb.ActorState_ACTOR_STATE_PAUSED,
			AssignedNode: node,
			Snapshots: []*ateapipb.Snapshot{{
				Uuid:       "snap-1",
				Generation: 1,
				Locality:   node,
			}},
		},
	})
}

func reportRequest(actorUID string) *ateapipb.ReportActorSnapshotStateRequest {
	durable := &ateapipb.SnapshotReport{
		Uuid:            "snap-1",
		Generation:      1,
		Fidelity:        ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:          &ateapipb.ObjectSnapshot{SnapshotUri: "gs://bucket/actors/a/snap-1"},
		LocalCommitTime: timestamppb.Now(),
	}
	return &ateapipb.ReportActorSnapshotStateRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUid: actorUID,
		State:    &ateapipb.ActorSnapshotState{LatestDurable: durable},
	}
}

// A report from the actor's node reaches the control plane, tagged with the
// node the caller's certificate proved, not anything the request claims.
func TestReportActorSnapshotState(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	actor := seedPausedActor(t, st, testNode)
	control := &fakeSuspender{}
	s := New(st, control, testAteletSPIFFEID, nil)

	req := reportRequest(actor.GetMetadata().GetUid())
	if _, err := s.ReportActorSnapshotState(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), req); err != nil {
		t.Fatalf("ReportActorSnapshotState() failed: %v", err)
	}
	if len(control.reports) != 1 {
		t.Fatalf("got %d applied reports, want 1", len(control.reports))
	}
	got := control.reports[0]
	if want := (resources.ActorRef{Atespace: suspendAtespace, Name: suspendActor}); got.actorRef != want {
		t.Errorf("actorRef = %v, want %v", got.actorRef, want)
	}
	if got.actorUID != actor.GetMetadata().GetUid() || got.node != testNode {
		t.Errorf("applied (uid %q, node %q), want (%q, %q)", got.actorUID, got.node, actor.GetMetadata().GetUid(), testNode)
	}
	if !proto.Equal(got.state, req.GetState()) {
		t.Errorf("applied state = %v, want %v", got.state, req.GetState())
	}
}

// Refusals happen before the report reaches the control plane. Actors on
// other nodes are reported as absent, so a caller learns nothing about them.
func TestReportActorSnapshotState_Refused(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	actor := seedPausedActor(t, st, testNode)
	uid := actor.GetMetadata().GetUid()
	onNode := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	invalid := reportRequest(uid)
	invalid.State = nil
	missing := reportRequest(uid)
	missing.Actor.Name = "no-such-actor"

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.ReportActorSnapshotStateRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), reportRequest(uid), codes.Unauthenticated},
		{"invalid request", onNode, invalid, codes.InvalidArgument},
		{"actor missing", onNode, missing, codes.NotFound},
		{"stale incarnation", onNode, reportRequest("1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17"), codes.NotFound},
		{"actor on another node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), reportRequest(uid), codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &fakeSuspender{}
			s := New(st, control, testAteletSPIFFEID, nil)
			_, err := s.ReportActorSnapshotState(tt.ctx, tt.req)
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("ReportActorSnapshotState() error = %v, want code %v", err, tt.want)
			}
			if len(control.reports) != 0 {
				t.Errorf("a refused report reached the control plane: %v", control.reports)
			}
		})
	}
}
