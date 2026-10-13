// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Fixed until they become flags or per-request settings on UploadConfig.
const (
	// snapshotUploadDelay is how long after the oldest not-yet-durable
	// snapshot of an actor was taken atelet uploads the actor's latest local
	// snapshot, under UPLOAD_MODE_DEFERRED.
	snapshotUploadDelay = 5 * time.Minute
	// actorDetachDelay is how long an actor stays suspended on this node, its
	// latest snapshot kept locally, before atelet detaches it.
	actorDetachDelay = 30 * time.Minute
	// snapshotRetryDelay is how long atelet holds an actor's background
	// uploads and detaches back after one fails. Each further consecutive
	// failure doubles it, up to maxSnapshotRetryDelay.
	snapshotRetryDelay    = time.Minute
	maxSnapshotRetryDelay = 10 * time.Minute
	// snapshotWorkers is how many background uploads and detaches run at once
	// on a node.
	snapshotWorkers = 4
)

// snapshotRef is one snapshot of an actor that atelet holds or has uploaded.
type snapshotRef struct {
	uuid       string
	generation int32
	fidelity   ateletpb.SnapshotFidelity
	uri        string
	// commitTime is when the checkpoint that produced the snapshot committed
	// on this node. Zero when this atelet did not take it.
	commitTime time.Time
	// uploadTime is when this atelet committed the snapshot's manifest to
	// object storage. Zero unless it did.
	uploadTime time.Time
}

func snapshotRefFrom(snap *ateletpb.Snapshot) snapshotRef {
	return snapshotRef{
		uuid:       snap.GetSnapshotUuid(),
		generation: snap.GetGeneration(),
		fidelity:   snap.GetFidelity(),
		uri:        snap.GetObject().GetSnapshotUri(),
	}
}

// report converts r for a field of ActorSnapshotState. Only latest_durable
// carries the upload time.
func (r *snapshotRef) report(withUploadTime bool) *ateletpb.SnapshotReport {
	if r == nil {
		return nil
	}
	rep := &ateletpb.SnapshotReport{
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: r.uuid,
			Fidelity:     r.fidelity,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: r.uri},
			Generation:   r.generation,
		},
	}
	if !r.commitTime.IsZero() {
		rep.LocalCommitTime = timestamppb.New(r.commitTime)
	}
	if withUploadTime && !r.uploadTime.IsZero() {
		rep.DurableCompleteTime = timestamppb.New(r.uploadTime)
	}
	return rep
}

// fidelityFromLabel parses the fidelity label a snapshot manifest records:
// UNSPECIFIED when it records none.
func fidelityFromLabel(label string) ateletpb.SnapshotFidelity {
	switch label {
	case ateattr.SnapshotFidelityVolumes:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateattr.SnapshotFidelityRootfs:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateattr.SnapshotFidelityMemory:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

// opKind is what an operation on an actor's snapshots does.
type opKind int

const (
	// opUpload makes a snapshot durable: a background upload, or a
	// PromoteSnapshot call.
	opUpload opKind = iota
	// opDetach makes the latest local snapshot durable, then deletes the
	// actor's local snapshots: a background detach, or a DetachActor call.
	opDetach
)

// actorOp is an upload or detach of an actor's snapshot in progress.
type actorOp struct {
	kind opKind
	// target is the snapshot it uploads or detaches.
	target snapshotRef
	cancel context.CancelFunc
	// done is closed once the operation has ended, and has deleted any local
	// snapshot it superseded.
	done chan struct{}
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by snapshotTracker.mu.
type actorSnapshots struct {
	actorUID string

	// newest is the highest-generation snapshot this node has seen for the
	// actor, either committed by Checkpoint or read by Restore. It differs
	// from latestLocal and latestDurable when an actor is restored on this
	// node from an external durable snapshot in object storage (where
	// latestLocal is nil and latestDurable is nil or from an earlier stay on
	// this node) or after a detach has cleared latestLocal. stale() compares
	// incoming Checkpoint and PromoteSnapshot requests against newest so an
	// out-of-order RPC with an older generation is rejected with Aborted.
	newest *snapshotRef
	// newestByCheckpoint is true when newest was recorded by a Checkpoint on
	// this node, and false when it was recorded by Restore or PromoteSnapshot.
	// beginCheckpoint uses this to distinguish an idempotent retry of the
	// just-completed Checkpoint (same generation and UUID, newestByCheckpoint
	// == true) from a stale Checkpoint that reuses the generation of the
	// snapshot the actor was restored from (newestByCheckpoint == false,
	// rejected with Aborted).
	newestByCheckpoint bool

	// latestLocal is the newest snapshot currently stored in the actor's
	// node-local checkpoint directory. Nil once detached or after an
	// UPLOAD_MODE_IMMEDIATE checkpoint.
	latestLocal *snapshotRef
	// latestDurable is the newest snapshot confirmed in object storage.
	latestDurable *snapshotRef

	// undurableSince is the commitTime of the oldest local checkpoint taken
	// since the last durable snapshot, or since the upload in progress
	// started. Zero when latestLocal is nil, already durable, or being
	// uploaded.
	//
	// Measuring uploadDelay from undurableSince rather than
	// latestLocal.commitTime ensures an actor that pauses and resumes
	// repeatedly still uploads within uploadDelay instead of postponing
	// durability indefinitely. A background upload of s1 clears
	// undurableSince when it starts; if a newer checkpoint s2 commits while s1
	// is still uploading, commitLocal sets undurableSince to s2.commitTime
	// (and any subsequent s3 keeps s2.commitTime). When s1's upload finishes,
	// undurableSince is already s2.commitTime, so s2 is due for upload at
	// s2.commitTime + uploadDelay (unless s1's upload failed, which restores
	// undurableSince to s1.commitTime).
	undurableSince time.Time
	// retryAt holds the actor's next background upload or detach back after
	// one failed.
	retryAt time.Time
	// detachAt is when atelet detaches the actor, if it is still suspended on
	// this node with a local snapshot then. Zero when no detach is scheduled.
	detachAt time.Time

	// running is set while the actor runs, or is being started, on this node.
	// While true, the actor is never detached.
	running bool

	// op is the actor's upload or detach in progress, background or requested
	// by an RPC, or nil: an actor has at most one at a time. Run, Restore,
	// Checkpoint, PromoteSnapshot, and DetachActor calls wait for a detach to
	// end via waitForDetachLocked so local files are not pruned underneath
	// them.
	op *actorOp
}

// stale reports whether a request for snap comes from a caller whose view of
// the actor is behind what atelet has seen.
func (a *actorSnapshots) stale(snap snapshotRef) bool {
	if a.newest == nil {
		return false
	}
	return snap.generation < a.newest.generation ||
		(snap.generation == a.newest.generation && snap.uuid != a.newest.uuid)
}

// durable reports whether a snapshot at least as new as ref is durable.
func (a *actorSnapshots) durable(ref *snapshotRef) bool {
	return ref != nil && a.latestDurable != nil && a.latestDurable.generation >= ref.generation
}

// holdsDurable reports whether latestDurable is ref, uploaded where ref names.
func (a *actorSnapshots) holdsDurable(ref snapshotRef) bool {
	return a.latestDurable != nil && a.latestDurable.uuid == ref.uuid && a.latestDurable.uri == ref.uri
}

// uploading returns the snapshot an upload in progress is making durable, or
// nil.
func (a *actorSnapshots) uploading() *snapshotRef {
	if a.op == nil || a.op.kind != opUpload {
		return nil
	}
	return &a.op.target
}

// uploadDue returns when atelet uploads the actor's latest local snapshot:
// uploadDelay after the oldest snapshot not yet durable was taken, or at
// retryAt if that is later. ok is false when there is nothing to upload: no
// local snapshot, one already durable, or one being uploaded.
func (a *actorSnapshots) uploadDue(uploadDelay time.Duration) (due time.Time, ok bool) {
	local := a.latestLocal
	if local == nil || a.durable(local) {
		return time.Time{}, false
	}
	if up := a.uploading(); up != nil && up.uuid == local.uuid {
		return time.Time{}, false
	}
	since := a.undurableSince
	if since.IsZero() {
		since = local.commitTime
	}
	return later(since.Add(uploadDelay), a.retryAt), true
}

// detachDue returns when atelet detaches the actor: at detachAt, or at
// retryAt if that is later. ok is false when it does not: no detach is
// scheduled, the actor runs on this node, or atelet holds no local snapshot of
// it.
func (a *actorSnapshots) detachDue() (due time.Time, ok bool) {
	if a.detachAt.IsZero() || a.running || a.latestLocal == nil {
		return time.Time{}, false
	}
	return later(a.detachAt, a.retryAt), true
}

func later(t, u time.Time) time.Time {
	if u.After(t) {
		return u
	}
	return t
}

// snapshotStore moves an actor's snapshots between this node and object
// storage.
type snapshotStore interface {
	// uploadLocalSnapshot makes ref durable at ref.uri from its local copy,
	// unless a manifest is already committed there. Either copy must hold at
	// least minFidelity. It returns the durable copy's manifest, and whether
	// it uploaded.
	uploadLocalSnapshot(ctx context.Context, actorUID string, ref snapshotRef, minFidelity ateletpb.SnapshotFidelity) (*sandboxAssetsRecord, bool, error)
}

// snapshotTracker tracks each actor's snapshots on this node, and uploads and
// detaches them in the background. It keeps each actor's state in the actor's
// snapshot state file, so a restarted atelet carries on with pending uploads
// and detaches: see load.
//
// Background work runs off a workqueue of actor UIDs. Each change to an
// actor's state queues the actor for when its next upload or detach is due:
//  1. A deferred checkpoint (commitLocal) records the new snapshot as
//     latestLocal. Its upload is due uploadDelay after undurableSince, and
//     the actor's detach detachDelay after the checkpoint.
//  2. A worker that takes the actor from the queue runs whatever is due for
//     it then, preferring a detach, which also makes latestLocal durable.
//     When nothing is due yet, it queues the actor again for when something
//     is: the queue holds an actor until the earliest time it was queued
//     for, so a due time that moves later only costs a worker that check.
//  3. The worker uploads the latestLocal of when it runs, so snapshots
//     superseded while the actor waits are never uploaded.
//  4. The queue hands an actor to one worker at a time, and the number of
//     workers bounds how many background uploads and detaches run at once.
//  5. An upload, background or by PromoteSnapshot, leaves latestLocal on disk
//     so a same-node Restore still reads it. A detach, background or by
//     DetachActor, makes latestLocal durable and then deletes all the actor's
//     local snapshots.
//
// PromoteSnapshot and DetachActor run in the calling goroutine rather than on
// a worker, so explicit control-plane calls do not wait behind other actors'
// background work. A call cancels an upload of an older snapshot in progress
// and waits for any other operation, and a worker leaves an actor with an
// operation in progress to that operation's end, which queues the actor
// again.
type snapshotTracker struct {
	// ctx bounds background uploads and detaches, and shuts the queue down
	// when it ends.
	ctx   context.Context
	store snapshotStore
	// clock is what due times are measured on, and the queue waits on.
	clock clock.WithTicker

	// The delays, retry backoff, and worker count are initialized from the
	// package constants in newSnapshotTracker, and overridden in tests.
	//
	// uploadDelay is how long after undurableSince a deferred local snapshot
	// becomes due for background upload.
	uploadDelay time.Duration
	// detachDelay is how long a suspended actor keeps its local snapshot on
	// this node before atelet detaches it.
	detachDelay time.Duration
	// retries says how long an actor's background work is held back after a
	// failure: longer with each consecutive failure for the actor.
	retries workqueue.TypedRateLimiter[string]
	// workers is how many background uploads and detaches run at once.
	workers int

	// queue holds the UIDs of actors with background work scheduled.
	queue workqueue.TypedDelayingInterface[string]
	// workersDone counts the workers still running.
	workersDone sync.WaitGroup

	mu sync.Mutex
	// actors maps actorUID to the snapshot tracking state for that actor.
	actors map[string]*actorSnapshots
}

// newSnapshotTracker returns a tracker over store that measures time on clk.
// Its workers run once start is called, until ctx ends.
func newSnapshotTracker(ctx context.Context, store snapshotStore, clk clock.WithTicker) *snapshotTracker {
	t := &snapshotTracker{
		ctx:         ctx,
		store:       store,
		clock:       clk,
		uploadDelay: snapshotUploadDelay,
		detachDelay: actorDetachDelay,
		retries:     workqueue.NewTypedItemExponentialFailureRateLimiter[string](snapshotRetryDelay, maxSnapshotRetryDelay),
		workers:     snapshotWorkers,
		queue:       workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk}),
		actors:      map[string]*actorSnapshots{},
	}
	context.AfterFunc(ctx, t.queue.ShutDown)
	return t
}

// start starts the workers that upload and detach actors' snapshots in the
// background.
func (t *snapshotTracker) start() {
	for range t.workers {
		t.workersDone.Go(t.runWorker)
	}
}

// wait blocks until the workers have stopped, after the tracker's context
// ends.
func (t *snapshotTracker) wait() {
	t.workersDone.Wait()
}

// runWorker runs the background work of the actors it takes from the queue,
// until the queue shuts down.
func (t *snapshotTracker) runWorker() {
	for {
		actorUID, shutdown := t.queue.Get()
		if shutdown {
			return
		}
		t.syncActor(actorUID)
		t.queue.Done(actorUID)
	}
}

// syncActor runs the actor's background detach or upload if one is due, and
// otherwise queues the actor for when one is.
func (t *snapshotTracker) syncActor(actorUID string) {
	t.mu.Lock()
	a := t.actors[actorUID]
	if a == nil || a.op != nil || t.ctx.Err() != nil {
		// Forgotten, or an operation is in progress, whose end queues the
		// actor again, or atelet is shutting down.
		t.mu.Unlock()
		return
	}
	now := t.clock.Now()
	if due, ok := a.detachDue(); ok && !due.After(now) {
		// The detach also uploads latestLocal unless it is durable.
		ctx, op, alreadyDurable := t.beginDetachLocked(t.ctx, a)
		t.mu.Unlock()
		if _, err := t.finishDetach(ctx, a, op, alreadyDurable, true); err != nil {
			slog.WarnContext(t.ctx, "Background detach failed", slog.String("actorUID", actorUID), slog.Any("err", err))
		}
		return
	}
	if due, ok := a.uploadDue(t.uploadDelay); ok && !due.After(now) {
		ctx, op := t.beginOpLocked(t.ctx, a, opUpload, *a.latestLocal)
		// The upload covers every snapshot taken so far.
		a.undurableSince = time.Time{}
		t.mu.Unlock()
		t.backgroundUpload(ctx, a, op)
		return
	}
	t.requeueLocked(a)
	t.mu.Unlock()
}

// requeueLocked queues the actor for when its next background upload or
// detach is due, if it has one.
func (t *snapshotTracker) requeueLocked(a *actorSnapshots) {
	due, ok := a.detachDue()
	if upload, uploadOK := a.uploadDue(t.uploadDelay); uploadOK && (!ok || upload.Before(due)) {
		due, ok = upload, true
	}
	if ok {
		t.queue.AddAfter(a.actorUID, due.Sub(t.clock.Now()))
	}
}

// beginOpLocked records an operation of kind on target as the actor's
// operation in progress. It returns the context the operation runs under,
// derived from ctx and ended by the operation's cancel.
func (t *snapshotTracker) beginOpLocked(ctx context.Context, a *actorSnapshots, kind opKind, target snapshotRef) (context.Context, *actorOp) {
	ctx, cancel := context.WithCancel(ctx)
	a.op = &actorOp{kind: kind, target: target, cancel: cancel, done: make(chan struct{})}
	return ctx, a.op
}

// beginDetachLocked begins a detach of the actor's latest local snapshot. It
// reports whether that snapshot is durable already, so the detach need not
// upload it.
func (t *snapshotTracker) beginDetachLocked(ctx context.Context, a *actorSnapshots) (context.Context, *actorOp, bool) {
	target := *a.latestLocal
	ctx, op := t.beginOpLocked(ctx, a, opDetach, target)
	return ctx, op, a.holdsDurable(target)
}

// endOpLocked records that op has ended, and queues the actor for its next
// background upload or detach. The caller closes op.done once it is done
// with the actor's local snapshots.
func (t *snapshotTracker) endOpLocked(a *actorSnapshots, op *actorOp) {
	op.cancel()
	a.op = nil
	if t.actors[a.actorUID] == a {
		t.requeueLocked(a)
	}
}

// waitOpLocked waits, releasing t.mu meanwhile, for op to end.
func (t *snapshotTracker) waitOpLocked(ctx context.Context, op *actorOp) error {
	t.mu.Unlock()
	defer t.mu.Lock()
	select {
	case <-op.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForDetachLocked waits, releasing t.mu meanwhile, until no detach of the
// actor is in progress. It returns the actor's entry, which may be nil, with
// t.mu held. Every RPC that reads or mutates an actor's local state calls this
// first so it never races with a detach pruning the actor's local directory.
func (t *snapshotTracker) waitForDetachLocked(ctx context.Context, actorUID string) (*actorSnapshots, error) {
	for {
		a := t.actors[actorUID]
		if a == nil || a.op == nil || a.op.kind != opDetach {
			return a, nil
		}
		if err := t.waitOpLocked(ctx, a.op); err != nil {
			return nil, err
		}
	}
}

// backOffLocked holds the actor's next background upload or detach back after
// one failed, longer with each consecutive failure, and returns until when.
func (t *snapshotTracker) backOffLocked(a *actorSnapshots) time.Time {
	a.retryAt = t.clock.Now().Add(t.retries.When(a.actorUID))
	return a.retryAt
}

// resetBackoffLocked lifts the hold backOffLocked put on the actor's
// background work.
func (t *snapshotTracker) resetBackoffLocked(a *actorSnapshots) {
	a.retryAt = time.Time{}
	t.retries.Forget(a.actorUID)
}

// actorLocked returns the actor's entry, creating it if needed.
func (t *snapshotTracker) actorLocked(actorUID string) *actorSnapshots {
	a := t.actors[actorUID]
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
		t.actors[actorUID] = a
	}
	return a
}

// stateLocked returns what atelet holds for the actor.
func (t *snapshotTracker) stateLocked(a *actorSnapshots) *ateletpb.ActorSnapshotState {
	st := &ateletpb.ActorSnapshotState{
		LatestLocal:   a.latestLocal.report(false),
		Uploading:     a.uploading().report(false),
		LatestDurable: a.latestDurable.report(true),
	}
	if due, ok := a.uploadDue(t.uploadDelay); ok {
		st.UploadDueTime = timestamppb.New(due)
	}
	return st
}

// state returns what atelet holds for the actor.
func (t *snapshotTracker) state(actorUID string) *ateletpb.ActorSnapshotState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if a := t.actors[actorUID]; a != nil {
		return t.stateLocked(a)
	}
	return &ateletpb.ActorSnapshotState{}
}

// beginCheckpoint decides how a Checkpoint for snap proceeds, once any detach
// in progress has finished. retry is set when atelet already committed snap,
// and an Aborted error is returned when the request is stale.
func (t *snapshotTracker) beginCheckpoint(ctx context.Context, actorUID string, snap snapshotRef) (retry bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a, err := t.waitForDetachLocked(ctx, actorUID)
	if err != nil || a == nil {
		return false, err
	}
	if a.newestByCheckpoint && snap.generation == a.newest.generation && snap.uuid == a.newest.uuid {
		return true, nil
	}
	if a.stale(snap) {
		return false, apierror.Aborted("snapshot %q at generation %d is stale: atelet holds snapshot %q at generation %d for this actor",
			snap.uuid, snap.generation, a.newest.uuid, a.newest.generation)
	}
	return false, nil
}

// commitLocal records snap, just stored locally under UPLOAD_MODE_DEFERRED, as
// the actor's latest local snapshot, and schedules its upload and the actor's
// detach. It returns the local snapshots to keep, snap and one still being
// uploaded, and the actor's state.
func (t *snapshotTracker) commitLocal(actorUID string, snap snapshotRef) ([]string, *ateletpb.ActorSnapshotState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.actorLocked(actorUID)
	newest, local := snap, snap
	a.newest, a.newestByCheckpoint = &newest, true
	a.running = false
	a.latestLocal = &local
	if a.undurableSince.IsZero() {
		a.undurableSince = snap.commitTime
	}
	a.detachAt = t.clock.Now().Add(t.detachDelay)
	keep := []string{snap.uuid}
	if up := a.uploading(); up != nil {
		keep = append(keep, up.uuid)
	}
	t.requeueLocked(a)
	return keep, t.stateLocked(a)
}

// commitDurable records snap, uploaded during Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest snapshot. The actor keeps no
// local snapshot: this cancels an older snapshot's upload, and waits, as long
// as ctx allows, for the upload to end and delete that snapshot's local copy.
// It returns the actor's state.
func (t *snapshotTracker) commitDurable(ctx context.Context, actorUID string, snap snapshotRef) *ateletpb.ActorSnapshotState {
	t.mu.Lock()
	a := t.actorLocked(actorUID)
	newest, durable := snap, snap
	a.newest, a.newestByCheckpoint = &newest, true
	a.running = false
	a.latestDurable = &durable
	a.latestLocal = nil
	a.undurableSince, a.detachAt = time.Time{}, time.Time{}
	t.resetBackoffLocked(a)
	op := a.op
	if op != nil {
		op.cancel()
	}
	t.mu.Unlock()

	if op != nil {
		select {
		case <-op.done:
		case <-ctx.Done():
		}
	}
	return t.state(actorUID)
}

// claimRunning marks the actor as starting on this node, once any detach in
// progress has finished, so atelet does not detach it while it runs. Call the
// returned func with whether it started.
func (t *snapshotTracker) claimRunning(ctx context.Context, actorUID string) (func(started bool), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.waitForDetachLocked(ctx, actorUID); err != nil {
		return nil, err
	}
	a := t.actorLocked(actorUID)
	a.running = true
	a.detachAt = time.Time{}
	return func(started bool) {
		t.mu.Lock()
		defer t.mu.Unlock()
		if started || t.actors[actorUID] != a {
			return
		}
		a.running = false
		if a.latestLocal != nil {
			a.detachAt = t.clock.Now().Add(t.detachDelay)
			t.requeueLocked(a)
		}
	}, nil
}

// restored records that the actor was restored from snap.
func (t *snapshotTracker) restored(actorUID string, snap snapshotRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.actorLocked(actorUID)
	if a.newest == nil || snap.generation > a.newest.generation {
		newest := snap
		a.newest, a.newestByCheckpoint = &newest, false
	}
}

// forget drops the actor's state, in memory and on disk, canceling any upload
// or detach in progress, and waits for that to stop. A worker that takes the
// actor from the queue afterwards finds nothing to do.
func (t *snapshotTracker) forget(ctx context.Context, actorUID string) error {
	t.mu.Lock()
	var op *actorOp
	if a := t.actors[actorUID]; a != nil {
		delete(t.actors, actorUID)
		if op = a.op; op != nil {
			op.cancel()
		}
	}
	t.mu.Unlock()
	t.retries.Forget(actorUID)

	if op != nil {
		select {
		case <-op.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// backgroundUpload runs op, a background upload of the actor's latest local
// snapshot when it began, on a worker. If a newer checkpoint superseded the
// snapshot meanwhile, it then deletes the snapshot's local copy.
func (t *snapshotTracker) backgroundUpload(ctx context.Context, a *actorSnapshots, op *actorOp) {
	target := op.target
	start := t.clock.Now()
	_, uploaded, err := t.store.uploadLocalSnapshot(ctx, a.actorUID, target, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED)
	attrs := []any{
		slog.String("actorUID", a.actorUID),
		slog.String("snapshot", target.uuid),
		slog.Int("generation", int(target.generation)),
		slog.Duration("elapsed", t.clock.Since(start)),
	}
	if err != nil {
		slog.WarnContext(ctx, "Background snapshot upload failed", append(attrs, slog.Any("err", err))...)
	} else {
		slog.InfoContext(ctx, "Background snapshot upload finished", append(attrs, slog.Bool("uploaded", uploaded))...)
	}

	t.mu.Lock()
	// A forgotten actor's local snapshots are left to Terminate, which
	// deletes them all.
	superseded := false
	if t.actors[a.actorUID] == a {
		switch {
		case err == nil:
			durable := target
			if uploaded {
				durable.uploadTime = t.clock.Now()
			}
			if a.latestDurable == nil || durable.generation > a.latestDurable.generation {
				a.latestDurable = &durable
			}
			t.resetBackoffLocked(a)
		case !a.durable(&target):
			// Restore undurableSince if no newer checkpoint set an earlier one.
			if a.undurableSince.IsZero() || target.commitTime.Before(a.undurableSince) {
				a.undurableSince = target.commitTime
			}
			// A call that canceled the upload decides what follows it.
			if ctx.Err() == nil {
				t.backOffLocked(a)
			}
		}
		superseded = a.latestLocal == nil || a.latestLocal.uuid != target.uuid
	}
	t.endOpLocked(a, op)
	t.mu.Unlock()

	if superseded {
		if err := removeLocalCheckpoint(t.ctx, a.actorUID, target.uuid); err != nil {
			slog.WarnContext(t.ctx, "failed to delete superseded local checkpoint", slog.String("actorUID", a.actorUID), slog.Any("err", err))
		}
	}
	close(op.done)
}

// promote makes want durable in object storage while keeping its local copy on
// this node. It runs in the calling goroutine rather than on a worker.
func (t *snapshotTracker) promote(ctx context.Context, actorUID string, want snapshotRef) (*ateletpb.ActorSnapshotState, error) {
	t.mu.Lock()
	var a *actorSnapshots
	for {
		var err error
		if a, err = t.waitForDetachLocked(ctx, actorUID); err != nil {
			t.mu.Unlock()
			return nil, err
		}
		if a != nil && a.running {
			t.mu.Unlock()
			return nil, apierror.Aborted("actor %s is running on this node", actorUID)
		}
		if a != nil && a.stale(want) {
			t.mu.Unlock()
			return nil, apierror.Aborted("snapshot %q at generation %d is stale: atelet holds snapshot %q at generation %d for this actor",
				want.uuid, want.generation, a.newest.uuid, a.newest.generation)
		}
		if a == nil || a.op == nil {
			break
		}
		// An upload is in progress: wait for an upload of want, and cancel an
		// older snapshot's first.
		if a.op.target.uuid != want.uuid {
			a.op.cancel()
		}
		if err := t.waitOpLocked(ctx, a.op); err != nil {
			t.mu.Unlock()
			return nil, err
		}
	}

	var target snapshotRef
	if a != nil && a.latestLocal != nil && a.latestLocal.uuid == want.uuid {
		target = *a.latestLocal
		target.uri = want.uri
	} else {
		// atelet does not track want locally, as after a detach or restart:
		// use its local copy on disk if there is one, or answer from its
		// manifest.
		target = want
	}
	untracked := a == nil
	if untracked {
		a = t.actorLocked(actorUID)
	}
	alreadyDurable := a.holdsDurable(target)
	pctx, op := t.beginOpLocked(ctx, a, opUpload, target)
	t.mu.Unlock()

	start := t.clock.Now()
	var rec *sandboxAssetsRecord
	uploaded := false
	err := pctx.Err()
	if err == nil {
		rec, uploaded, err = t.store.uploadLocalSnapshot(pctx, actorUID, target, want.fidelity)
	}
	if err == nil && rec != nil {
		if got := fidelityFromLabel(rec.Fidelity); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
			target.fidelity = got
		}
	}

	superseded := false
	t.mu.Lock()
	if t.actors[actorUID] == a {
		switch {
		case err == nil:
			d := target
			switch {
			case uploaded:
				d.uploadTime = t.clock.Now()
			case alreadyDurable:
				d.uploadTime = a.latestDurable.uploadTime
			}
			if a.latestDurable == nil || d.generation >= a.latestDurable.generation {
				a.latestDurable = &d
			}
			if a.newest == nil || d.generation > a.newest.generation {
				newest := d
				a.newest, a.newestByCheckpoint = &newest, false
			}
			if a.latestLocal != nil && a.latestLocal.uuid == d.uuid {
				a.latestLocal.fidelity = d.fidelity
				a.latestLocal.uri = d.uri
			}
			if a.latestLocal == nil || a.latestLocal.generation <= d.generation {
				a.undurableSince = time.Time{}
				t.resetBackoffLocked(a)
			}
			superseded = a.latestLocal != nil && a.latestLocal.uuid != target.uuid
		case untracked:
			delete(t.actors, actorUID)
		}
	}
	t.endOpLocked(a, op)
	st := t.stateLocked(a)
	t.mu.Unlock()

	if superseded {
		if rerr := removeLocalCheckpoint(t.ctx, actorUID, target.uuid); rerr != nil {
			slog.WarnContext(t.ctx, "failed to delete superseded local checkpoint", slog.String("actorUID", actorUID), slog.Any("err", rerr))
		}
	}
	close(op.done)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Promoted snapshot",
		slog.String("actorUID", actorUID),
		slog.String("snapshot", target.uuid),
		slog.Int("generation", int(target.generation)),
		slog.Bool("uploaded", uploaded),
		slog.Duration("elapsed", t.clock.Since(start)))
	return st, nil
}

// detach makes the actor's latest local snapshot durable, then deletes the
// actor's local snapshots. It is a no-op when atelet holds no local snapshot
// for the actor, and fails for an actor running on this node. It runs in the
// calling goroutine rather than on a worker.
func (t *snapshotTracker) detach(ctx context.Context, actorUID string) (*ateletpb.ActorSnapshotState, error) {
	t.mu.Lock()
	var a *actorSnapshots
	for {
		var err error
		if a, err = t.waitForDetachLocked(ctx, actorUID); err != nil {
			t.mu.Unlock()
			return nil, err
		}
		if a != nil && a.running {
			t.mu.Unlock()
			return nil, apierror.Aborted("actor %s is running on this node", actorUID)
		}
		if a == nil || a.latestLocal == nil {
			st := &ateletpb.ActorSnapshotState{}
			if a != nil {
				st = t.stateLocked(a)
			}
			t.mu.Unlock()
			return st, nil
		}
		if a.op == nil {
			break
		}
		// An upload is in progress: wait for an upload of the latest local
		// snapshot, which the detach then need not repeat, and cancel an
		// older snapshot's first.
		if a.op.target.uuid != a.latestLocal.uuid {
			a.op.cancel()
		}
		if err := t.waitOpLocked(ctx, a.op); err != nil {
			t.mu.Unlock()
			return nil, err
		}
	}
	dctx, op, alreadyDurable := t.beginDetachLocked(ctx, a)
	t.mu.Unlock()
	return t.finishDetach(dctx, a, op, alreadyDurable, false)
}

// finishDetach runs op, a detach of the actor's latest local snapshot: it
// uploads the snapshot unless alreadyDurable, then deletes the actor's local
// snapshots. background reports whether a worker runs it, rather than a
// DetachActor call.
func (t *snapshotTracker) finishDetach(ctx context.Context, a *actorSnapshots, op *actorOp, alreadyDurable, background bool) (*ateletpb.ActorSnapshotState, error) {
	target := op.target
	start := t.clock.Now()
	var rec *sandboxAssetsRecord
	uploaded := false
	err := ctx.Err()
	if err == nil && !alreadyDurable {
		rec, uploaded, err = t.store.uploadLocalSnapshot(ctx, a.actorUID, target, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED)
	}
	if err == nil && rec != nil {
		if got := fidelityFromLabel(rec.Fidelity); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
			target.fidelity = got
		}
	}

	// Record durability and clear latestLocal before pruning disk so a crash
	// during prune does not leave snapshot-state.json pointing at a partially
	// deleted local checkpoint. Callers waiting for the detach stay blocked
	// until pruning finishes below.
	t.mu.Lock()
	tracked := t.actors[a.actorUID] == a
	if tracked {
		switch {
		case err == nil:
			d := target
			switch {
			case uploaded:
				d.uploadTime = t.clock.Now()
			case alreadyDurable:
				d.uploadTime = a.latestDurable.uploadTime
			}
			if a.latestDurable == nil || d.generation >= a.latestDurable.generation {
				a.latestDurable = &d
			}
			a.latestLocal = nil
			a.undurableSince, a.detachAt = time.Time{}, time.Time{}
			t.resetBackoffLocked(a)
		case ctx.Err() == nil:
			// Detach again, and upload no sooner, once the retry delay has
			// passed.
			a.detachAt = t.backOffLocked(a)
		}
	}
	t.mu.Unlock()

	if err == nil && tracked {
		if perr := pruneLocalCheckpoints(t.ctx, a.actorUID); perr != nil {
			slog.WarnContext(t.ctx, "failed to delete local checkpoints after detach", slog.String("actorUID", a.actorUID), slog.Any("err", perr))
		}
	}

	t.mu.Lock()
	t.endOpLocked(a, op)
	st := t.stateLocked(a)
	t.mu.Unlock()
	close(op.done)

	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Detached actor",
		slog.String("actorUID", a.actorUID),
		slog.String("snapshot", target.uuid),
		slog.Int("generation", int(target.generation)),
		slog.Bool("uploaded", uploaded),
		slog.Bool("requested", !background),
		slog.Duration("elapsed", t.clock.Since(start)))
	return st, nil
}
