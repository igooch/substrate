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
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ApplyActorSnapshotState records the snapshot state that the atelet on node
// reports for the Actor. The caller has authenticated that atelet.
func (s *RPCService) ApplyActorSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	setSpanActorRefAttributes(ctx, actorRef)
	return s.actorWorkflow.ApplyReportedSnapshotState(ctx, actorRef, actorUID, node, state)
}

// ApplyReportedSnapshotState applies a report from the atelet on node to the
// Actor. It holds the Actor's lease and applies reports only to a SUSPENDED or
// RUNNING Actor, so a report never interleaves with a workflow. A report that
// changes nothing writes nothing.
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
	// Refuse with NotFound so an atelet learns nothing about Actors on other
	// nodes.
	if got := before.GetMetadata().GetUid(); got != actorUID {
		slog.WarnContext(ctx, "Refusing a snapshot state report naming a stale actor incarnation",
			slog.Any("actor", actorRef),
			slog.String("requested_uid", actorUID),
			slog.String("actual_uid", got),
			slog.String("caller_node", node))
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	if !actorOnNode(before, node) {
		slog.WarnContext(ctx, "Refusing a snapshot state report for an actor not on the caller's node",
			slog.Any("actor", actorRef),
			slog.String("caller_node", node),
			slog.String("assigned_node", before.GetStatus().GetAssignedNode()))
		return apierror.NotFound("Actor %s not found", actorRef)
	}

	// A workflow owns a transitioning Actor's snapshots, even after its lease
	// lapses. A crashed Actor's snapshots wait for a revert.
	switch st := before.GetStatus().GetState(); st {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_RUNNING:
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return apierror.FailedPrecondition("actor %s is %s; revert it first", actorRef, st)
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING, ateapipb.ActorState_ACTOR_STATE_PAUSING, ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_DELETING, ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return apierror.Aborted("actor %s is %s; retry once that finishes", actorRef, st)
	default:
		return apierror.Internal("actor %s has unexpected state %s", actorRef, st)
	}

	applied := proto.Clone(before).(*ateapipb.Actor)
	if err := applyActorSnapshotState(applied.GetStatus(), node, state); err != nil {
		return err
	}
	if proto.Equal(applied.GetStatus(), before.GetStatus()) {
		return nil
	}
	after, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(before), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status = applied.GetStatus()
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		return err
	}
	if after.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
		logActorCrashed(ctx, after, ateattr.OperationSuspend)
		recordActorCrash(ctx, ateattr.ActorMetricAttributes(before, "", ateattr.OperationSuspend))
	}

	for _, uri := range replacedDurableURIs(before, after, state) {
		if err := w.releaseReportedSnapshot(leaseCtx, before, uri); err != nil {
			slog.WarnContext(ctx, "Failed to release a replaced external snapshot after a snapshot state report; its objects are left in storage until the actor is deleted",
				slog.Any("actor", actorRef),
				slog.String("uri", uri),
				slog.String("err", err.Error()))
		}
	}
	return nil
}

// releaseReportedSnapshot deletes the objects under raw if the Actor owns
// them. A snapshot borrowed from a tag belongs to the tag.
func (w *ActorWorkflow) releaseReportedSnapshot(ctx context.Context, actor *ateapipb.Actor, raw string) error {
	if w.snapshotPlugin == nil {
		return nil
	}
	uri, err := resources.ParseSnapshotURI(raw)
	if err != nil {
		return fmt.Errorf("while parsing the replaced external snapshot %q: %w", raw, err)
	}
	if !uri.OwnedBy(actorSnapshotOwner(actor)) {
		return nil
	}
	return w.cleanupSnapshot(ctx, uri.Prefix())
}

// replacedDurableURIs returns the upload URIs of snapshots the report removed
// from the actor, so their objects can be deleted. It skips the snapshot atelet
// is still uploading.
func replacedDurableURIs(before, after *ateapipb.Actor, state *ateapipb.ActorSnapshotState) []string {
	keep := map[string]bool{state.GetUploading().GetObject().GetSnapshotUri(): true}
	for _, snap := range after.GetStatus().GetSnapshots() {
		keep[snap.GetDurableSnapshot().GetObject().GetSnapshotUri()] = true
	}
	var replaced []string
	for _, snap := range before.GetStatus().GetSnapshots() {
		uri := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
		if uri != "" && !keep[uri] {
			replaced = append(replaced, uri)
		}
	}
	return replaced
}

// actorOnNode reports whether the Actor is placed on node or has a local
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

// applyActorSnapshotState updates status to match the report from the atelet
// on node.
func applyActorSnapshotState(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) error {
	if err := checkReportedLocal(status, node, state); err != nil {
		return err
	}
	if err := applyLatestDurable(status, node, state.GetLatestDurable()); err != nil {
		return err
	}
	// A report with neither latest_local nor uploading is a detach. A report
	// with an upload in flight isn't.
	if state.GetLatestLocal() == nil && state.GetUploading() == nil {
		applyDetach(status, node)
	} else {
		dropUnlistedLocalSnapshots(status, node, state)
	}
	pruneReportedSnapshots(status)
	return nil
}

// checkReportedLocal checks the report's latest_local and uploading against
// status. It refuses a latest_local that status doesn't have, and a recorded
// uploading snapshot whose upload URI is missing or different. An uploading
// snapshot that status no longer has passes, because a newer checkpoint can
// drop its entry while atelet still uploads it.
func checkReportedLocal(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) error {
	if local := state.GetLatestLocal(); local != nil {
		snap, err := recordedSnapshot(status, node, local)
		if err != nil {
			return err
		}
		if snap == nil {
			return apierror.FailedPrecondition("local snapshot generation %d (%s) is not recorded for this actor", local.GetGeneration(), local.GetUuid())
		}
	}
	if up := state.GetUploading(); up != nil {
		snap, err := recordedSnapshot(status, node, up)
		if err != nil {
			return err
		}
		if snap != nil {
			recorded := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
			if recorded == "" {
				return apierror.FailedPrecondition("snapshot generation %d has no upload location recorded", up.GetGeneration())
			}
			if got := up.GetObject().GetSnapshotUri(); got != recorded {
				return apierror.InvalidArgument("snapshot generation %d is uploading to %q, want %q", up.GetGeneration(), got, recorded)
			}
		}
	}
	return nil
}

// recordedSnapshot returns the snapshot in status that r names, or nil if r's
// generation isn't in status. It refuses a snapshot recorded under another uuid
// or held on another node.
func recordedSnapshot(status *ateapipb.ActorStatus, node string, r *ateapipb.SnapshotReport) (*ateapipb.Snapshot, error) {
	snap := findSnapshotByGeneration(status.GetSnapshots(), r.GetGeneration())
	if snap == nil {
		return nil, nil
	}
	if snap.GetUuid() != r.GetUuid() {
		return nil, apierror.FailedPrecondition("snapshot generation %d is recorded as %s, not %s", r.GetGeneration(), snap.GetUuid(), r.GetUuid())
	}
	if locality := snap.GetLocality(); locality != "" && locality != node {
		return nil, apierror.FailedPrecondition("snapshot generation %d is held on node %s, not %s", r.GetGeneration(), locality, node)
	}
	return snap, nil
}

// applyLatestDurable marks the reported durable snapshot COMPLETED and records
// the fidelity from its manifest. The snapshot must be held on node, and its
// URI must match the recorded upload URI. A report that isn't newer than the
// latest COMPLETED snapshot changes nothing, and neither does one for a
// generation that status no longer has, such as one a revert discarded.
func applyLatestDurable(status *ateapipb.ActorStatus, node string, reported *ateapipb.SnapshotReport) error {
	if reported == nil {
		return nil
	}
	snap, err := recordedSnapshot(status, node, reported)
	if err != nil {
		return err
	}
	if snap == nil {
		if reported.GetGeneration() <= status.GetLastAssignedGeneration() {
			return nil
		}
		return apierror.FailedPrecondition("snapshot generation %d (%s) is not recorded for this actor", reported.GetGeneration(), reported.GetUuid())
	}
	want := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if want == "" {
		return apierror.FailedPrecondition("snapshot generation %d has no upload location recorded", reported.GetGeneration())
	}
	if got := reported.GetObject().GetSnapshotUri(); got != want {
		return apierror.InvalidArgument("snapshot generation %d was uploaded to %q, want %q", reported.GetGeneration(), got, want)
	}
	completed := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if completed != nil && reported.GetGeneration() <= completed.GetGeneration() {
		return nil
	}
	if snap.GetLocality() == "" {
		return apierror.FailedPrecondition("snapshot generation %d is not held on node %s", reported.GetGeneration(), node)
	}
	snap.Fidelity = reported.GetFidelity()
	snap.DurableSnapshot = &ateapipb.SnapshotStorage{
		Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: want},
	}
	return nil
}

// applyDetach records that the atelet on node deleted its copies of the
// Actor's snapshots, and releases a SUSPENDED Actor from node. If they held the
// only copy of its latest snapshot, it marks the Actor CRASHED rather than let
// it resume from an older one.
func applyDetach(status *ateapipb.ActorStatus, node string) {
	onNode := latestSnapshot(status, func(s *ateapipb.Snapshot) bool { return s.GetLocality() == node })
	lost := onNode != nil &&
		onNode.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED &&
		onNode == latestSnapshot(status, func(*ateapipb.Snapshot) bool { return true })
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == node {
			snap.Locality = ""
		}
	}
	if st := status.GetState(); st != ateapipb.ActorState_ACTOR_STATE_SUSPENDED && st != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		// A running Actor stays placed on node.
		return
	}
	if status.GetAssignedNode() == node {
		status.AssignedNode = ""
	}
	if lost {
		status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		status.Crash = newActorCrash(ateattr.OperationSuspend, fmt.Sprintf("atelet on node %s deleted snapshot generation %d before it was durable", node, onNode.GetGeneration()))
	}
}

// dropUnlistedLocalSnapshots clears node's locality on older snapshots that the
// report doesn't list. atelet deleted them and won't upload them.
func dropUnlistedLocalSnapshots(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) {
	listed := map[string]bool{}
	var newest int32
	for _, r := range []*ateapipb.SnapshotReport{state.GetLatestLocal(), state.GetUploading()} {
		if r != nil {
			listed[r.GetUuid()] = true
			newest = max(newest, r.GetGeneration())
		}
	}
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == node && !listed[snap.GetUuid()] && snap.GetGeneration() < newest {
			snap.Locality = ""
		}
	}
}

// pruneReportedSnapshots removes older COMPLETED snapshots, and snapshots that
// no node holds and that never became durable, such as an abandoned upload. It
// keeps the newest generation and doesn't change any locality.
func pruneReportedSnapshots(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		superseded := snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED && snap != latestDurable
		unheld := snap.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED && snap.GetLocality() == ""
		if snap.GetLocality() != "" || snap.GetGeneration() == status.GetLastAssignedGeneration() || !(superseded || unheld) {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}
