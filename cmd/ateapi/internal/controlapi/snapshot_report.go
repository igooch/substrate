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

	// A snapshot with no durable location recorded, such as a paused actor's,
	// is uploaded where suspend would put it. The template cannot move it:
	// UpdateActor replaces a template only while the actor is SUSPENDED, and a
	// paused actor is PAUSED.
	expectedURI := func(uuid string) (string, error) {
		return expectedSnapshotURI(leaseCtx, w.store, before, uuid)
	}
	after, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(before), func(toUpdate *ateapipb.Actor) error {
		return applyActorSnapshotState(toUpdate.GetStatus(), node, state, expectedURI)
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

// expectedSnapshotURI returns where suspend would upload the actor's snapshot
// named uuid. A missing template is a refusal; any other error is returned as
// is, so atelet retries the report.
func expectedSnapshotURI(ctx context.Context, templates actorTemplateGetter, actor *ateapipb.Actor, uuid string) (string, error) {
	tmpl, err := resolveActorTemplate(ctx, templates, actor)
	if errors.Is(err, errActorTemplateNotFound) {
		return "", apierror.FailedPrecondition("%v", err)
	}
	if err != nil {
		return "", err
	}
	uri, err := newInProgressSnapshotURI(tmpl, actor, uuid)
	if err != nil {
		return "", apierror.FailedPrecondition("%v", err)
	}
	return uri.String(), nil
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
// atelet on node reports. expectedURI says where a snapshot with no durable
// location recorded is uploaded.
func applyActorSnapshotState(status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState, expectedURI func(uuid string) (string, error)) error {
	if err := checkReportedLocal(status, node, state); err != nil {
		return err
	}
	if err := applyLatestDurable(status, node, state.GetLatestDurable(), expectedURI); err != nil {
		return err
	}
	// atelet holds nothing locally once it has detached. A report with an
	// upload still in flight is not a detach, even with no latest_local.
	if state.GetLatestLocal() == nil && state.GetUploading() == nil {
		if err := applyDetach(status, node); err != nil {
			return err
		}
	} else {
		dropUnlistedLocalSnapshots(status, node, state)
	}
	pruneReportedSnapshots(status)
	return nil
}

// checkReportedLocal refuses a report naming a local snapshot the control plane
// does not record on the caller's node. An upload of a snapshot the record has
// since dropped is allowed: pause drops an older local snapshot while atelet
// still uploads it. latest_local only checks the report: the control plane
// places local snapshots, so a report never adds a locality.
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
		if _, err := recordedSnapshot(status, node, up); err != nil {
			return err
		}
	}
	return nil
}

// recordedSnapshot returns the recorded snapshot r names, or nil if its
// generation is not recorded. A recorded generation under another uuid, or
// held on another node, is refused: a report speaks only for its own node.
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

// applyLatestDurable records the reported durable snapshot as COMPLETED, with
// the fidelity its manifest records. A duplicate, or a report older than the
// latest COMPLETED snapshot, changes nothing. So does an upload of a
// generation the record no longer holds, such as one a revert discarded;
// actor deletion collects its objects. Only a snapshot recorded on the
// caller's node can newly become COMPLETED: no other node uploaded it.
func applyLatestDurable(status *ateapipb.ActorStatus, node string, reported *ateapipb.SnapshotReport, expectedURI func(uuid string) (string, error)) error {
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
		if want, err = expectedURI(reported.GetUuid()); err != nil {
			return err
		}
	}
	if got := reported.GetObject().GetSnapshotUri(); got != want {
		return apierror.InvalidArgument("snapshot generation %d was uploaded to %q, want %q", reported.GetGeneration(), got, want)
	}
	completed := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if completed != nil && reported.GetGeneration() <= completed.GetGeneration() {
		return nil
	}
	if snap.GetLocality() != node {
		return apierror.FailedPrecondition("snapshot generation %d is not held on node %s", reported.GetGeneration(), node)
	}
	snap.Fidelity = reported.GetFidelity()
	snap.DurableSnapshot = &ateapipb.SnapshotStorage{
		Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: want},
	}
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
	return nil
}

// dropUnlistedLocalSnapshots clears the locality of older snapshots on node
// that atelet no longer lists: it has deleted them and will never upload them.
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

// pruneReportedSnapshots drops the entries a report has made useless: a
// COMPLETED durable snapshot older than the latest one, and a local-only
// snapshot no node holds anymore. It keeps every snapshot a node holds, every
// pending upload, and the newest generation, which a workflow may still be
// filling in. Unlike pruneSnapshots, it touches no locality: a report speaks
// only for its own node.
func pruneReportedSnapshots(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		superseded := snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED && snap != latestDurable
		unheld := snap.GetDurableSnapshot() == nil && snap.GetLocality() == ""
		if snap.GetLocality() != "" || snap.GetGeneration() == status.GetLastAssignedGeneration() || !(superseded || unheld) {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}
