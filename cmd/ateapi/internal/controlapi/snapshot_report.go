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

package controlapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ApplyActorSnapshotState records the snapshot state the atelet on node
// reports for the Actor. The caller has authenticated that atelet.
func (s *RPCService) ApplyActorSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	setSpanActorRefAttributes(ctx, actorRef)
	return s.actorWorkflow.ApplyReportedSnapshotState(ctx, actorRef, actorUID, node, state)
}

// ApplyReportedSnapshotState applies state under the Actor's lease, so it
// never interleaves with a workflow that reads and rewrites the Actor's
// snapshots. A report sent while a workflow runs gets Aborted, and atelet
// retries it with whatever its state is by then.
func (w *ActorWorkflow) ApplyReportedSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return err
	}
	defer lease.Close()

	before, err := w.store.GetActor(leaseCtx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return apierror.NotFound("Actor %s not found", actorRef)
		}
		return err
	}
	// Every refusal is NotFound, so an atelet learns nothing about Actors on
	// other nodes.
	if before.GetMetadata().GetUid() != actorUID || !actorOnNode(before, node) {
		slog.WarnContext(ctx, "Refusing a snapshot state report for an actor not on the caller's node",
			slog.Any("actor", actorRef),
			slog.String("requested_uid", actorUID),
			slog.String("caller_node", node))
		return apierror.NotFound("Actor %s not found", actorRef)
	}

	// Apply to a copy first, so a refused report returns its own error rather
	// than one wrapped by the store.
	if err := applyActorSnapshotState(proto.Clone(before).(*ateapipb.Actor).GetStatus(), node, state); err != nil {
		return err
	}
	after, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(before), func(toUpdate *ateapipb.Actor) error {
		return applyActorSnapshotState(toUpdate.GetStatus(), node, state)
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		return err
	}

	if err := w.releaseStaleSnapshots(leaseCtx, before, after, releaseView(state)); err != nil {
		slog.WarnContext(ctx, "Failed to release stale external snapshots after a snapshot state report; their objects are left in storage until the actor is deleted",
			slog.Any("actor", actorRef),
			slog.String("err", err.Error()))
	}
	return nil
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

// applyActorSnapshotState updates status to match the snapshot state the
// atelet on node reports.
func applyActorSnapshotState(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) error {
	if err := applyLatestDurable(status, state.GetLatestDurable()); err != nil {
		return err
	}
	if state.GetLatestLocal() == nil {
		return applyDetach(status, node)
	}
	dropUnlistedLocalSnapshots(status, node, state)
	pruneSnapshots(status, findLatestLocalSnapshot(status))
	return nil
}

// applyLatestDurable marks the reported durable snapshot COMPLETED. A report
// no newer than what is recorded changes nothing: it is a duplicate, or was
// overtaken by a later one.
func applyLatestDurable(status *ateapipb.ActorStatus, reported *ateapipb.SnapshotReport) error {
	if reported == nil {
		return nil
	}
	completed := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if completed != nil && reported.GetGeneration() <= completed.GetGeneration() {
		return nil
	}
	snap := findSnapshotByGeneration(status.GetSnapshots(), reported.GetGeneration())
	if snap == nil || snap.GetUuid() != reported.GetUuid() {
		return apierror.FailedPrecondition("snapshot generation %d (%s) is not recorded for this actor", reported.GetGeneration(), reported.GetUuid())
	}
	recorded := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if recorded == "" {
		return apierror.FailedPrecondition("snapshot generation %d has no durable location recorded", reported.GetGeneration())
	}
	if got := reported.GetObject().GetSnapshotUri(); got != recorded {
		return apierror.InvalidArgument("snapshot generation %d was uploaded to %q, want %q", reported.GetGeneration(), got, recorded)
	}
	snap.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	return nil
}

// applyDetach records that atelet no longer serves local restores for the
// Actor on node. It refuses to drop the only copy of the Actor's latest
// snapshot on node.
func applyDetach(status *ateapipb.ActorStatus, node string) error {
	if latest := latestSnapshot(status, func(s *ateapipb.Snapshot) bool { return s.GetLocality() == node }); latest != nil &&
		latest.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
		return apierror.FailedPrecondition("snapshot generation %d on node %s is not durable yet", latest.GetGeneration(), node)
	}
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == node {
			snap.Locality = ""
		}
	}
	// A resuming Actor's assigned_node belongs to the resume workflow, which
	// holds the lease this report waits on. Only an idle Actor is released
	// from the node.
	switch status.GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED:
		if status.GetAssignedNode() == node {
			status.AssignedNode = ""
		}
	}
	pruneSnapshots(status, findLatestLocalSnapshot(status))
	return nil
}

// dropUnlistedLocalSnapshots clears the locality of older snapshots on node
// that atelet no longer lists: it has deleted them and will never upload them.
func dropUnlistedLocalSnapshots(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) {
	listed := map[string]bool{state.GetLatestLocal().GetUuid(): true}
	if up := state.GetUploading(); up != nil {
		listed[up.GetUuid()] = true
	}
	newest := state.GetLatestLocal().GetGeneration()
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == node && !listed[snap.GetUuid()] && snap.GetGeneration() < newest {
			snap.Locality = ""
		}
	}
}

// releaseView carries the parts of state releaseStaleSnapshots reads.
func releaseView(state *ateapipb.ActorSnapshotState) *ateletpb.ActorSnapshotState {
	view := func(r *ateapipb.SnapshotReport) *ateletpb.SnapshotReport {
		if r == nil {
			return nil
		}
		return &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: r.GetUuid(),
			Generation:   r.GetGeneration(),
			Object:       &ateletpb.ObjectStorage{SnapshotUri: r.GetObject().GetSnapshotUri()},
		}}
	}
	return &ateletpb.ActorSnapshotState{
		Uploading:     view(state.GetUploading()),
		LatestDurable: view(state.GetLatestDurable()),
	}
}
