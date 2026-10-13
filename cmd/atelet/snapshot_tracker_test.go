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
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	testingclock "k8s.io/utils/clock/testing"
)

const trackedActorUID = "actor-uid-1"

const noFidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED

// The delays of the trackers tests build. Nothing is due until a test advances
// the tracker's clock.
const (
	testUploadDelay   = time.Hour
	testDetachDelay   = 4 * time.Hour
	testRetryDelay    = 30 * time.Minute
	testMaxRetryDelay = 2 * time.Hour
)

// uploadCall is one call to fakeSnapshotStore.uploadLocalSnapshot.
type uploadCall struct {
	uuid        string
	minFidelity ateletpb.SnapshotFidelity
}

// fakeSnapshotStore is a snapshotStore that records its calls. A call for a
// URI it already holds uploads nothing.
type fakeSnapshotStore struct {
	// started receives each call's snapshot uuid as the call starts.
	started chan string
	// release ends a blocked call with the error it receives.
	release chan error

	mu       sync.Mutex
	calls    []uploadCall
	uploaded map[string]bool
	blocking bool
	err      error
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{started: make(chan string, 100), release: make(chan error)}
}

// setBlocking makes later calls wait for release, or their context.
func (f *fakeSnapshotStore) setBlocking(blocking bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocking = blocking
}

// setErr makes later calls fail with err, or succeed when it is nil.
func (f *fakeSnapshotStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSnapshotStore) uploads() []uploadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeSnapshotStore) uploadLocalSnapshot(ctx context.Context, _ string, ref snapshotRef, minFidelity ateletpb.SnapshotFidelity) (*sandboxAssetsRecord, bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, uploadCall{ref.uuid, minFidelity})
	blocking, err := f.blocking, f.err
	f.mu.Unlock()
	f.started <- ref.uuid
	if blocking {
		select {
		case err = <-f.release:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	if err != nil {
		return nil, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := &sandboxAssetsRecord{Fidelity: ateattr.SnapshotFidelityValue(ref.fidelity)}
	if f.uploaded[ref.uri] {
		return rec, false, nil
	}
	if f.uploaded == nil {
		f.uploaded = map[string]bool{}
	}
	f.uploaded[ref.uri] = true
	return rec, true, nil
}

// newTestTracker returns a tracker over store, with its workers running, that
// measures time on a fake clock: background work starts only once a test
// advances the clock to when it is due (see stepTo). The tracked actor has a
// directory, where the tracker saves its state.
func newTestTracker(t *testing.T, store snapshotStore) *snapshotTracker {
	t.Helper()
	tr := newIdleTestTracker(t, store)
	tr.start()
	return tr
}

// newIdleTestTracker returns a tracker like newTestTracker's whose workers
// have not started.
func newIdleTestTracker(t *testing.T, store snapshotStore) *snapshotTracker {
	t.Helper()
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	return newTestTrackerOnNode(t, store)
}

// newTestTrackerOnNode returns a tracker like newIdleTestTracker's, over the
// node directories already in use: as a restarted atelet builds before it
// loads and starts. Its workers stop once the test ends, before the node
// directories are restored.
func newTestTrackerOnNode(t *testing.T, store snapshotStore) *snapshotTracker {
	t.Helper()
	tr := newSnapshotTracker(t.Context(), store, testingclock.NewFakeClock(time.Now()))
	tr.uploadDelay, tr.detachDelay = testUploadDelay, testDetachDelay
	tr.retries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testRetryDelay, testMaxRetryDelay)
	t.Cleanup(tr.wait)
	return tr
}

// stepTo advances the tracker's fake clock to at, unless it is there already,
// so the background work due by then starts.
func stepTo(tr *snapshotTracker, at time.Time) {
	clk := tr.clock.(*testingclock.FakeClock)
	if d := at.Sub(clk.Now()); d > 0 {
		clk.Step(d)
	}
}

// testRef is a MEMORY snapshot of the tracked actor.
func testRef(uuid string, generation int32, commitTime time.Time) snapshotRef {
	return snapshotRef{
		uuid:       uuid,
		generation: generation,
		fidelity:   ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		uri:        "gs://test-bucket/atespaces/ate-demo/actors/" + trackedActorUID + "/snapshots/" + uuid,
		commitTime: commitTime,
	}
}

// waitStarted waits for the next store call to start, and returns the uuid of
// the snapshot it uploads.
func waitStarted(t *testing.T, store *fakeSnapshotStore) string {
	t.Helper()
	select {
	case uuid := <-store.started:
		return uuid
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an upload to start")
		return ""
	}
}

// opDone returns a channel closed once the actor's upload or detach in
// progress has ended, deleting a superseded local snapshot included.
func opDone(t *testing.T, tr *snapshotTracker, actorUID string) <-chan struct{} {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	a := tr.actors[actorUID]
	if a == nil || a.op == nil {
		t.Fatal("no upload or detach in progress")
	}
	return a.op.done
}

func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// eventually polls cond until it holds, and fails the test if it does not
// within 10 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// startUpload advances the clock to when the tracked actor's upload is due,
// and waits for a worker to start it. It returns a channel closed once the
// upload has ended.
func startUpload(t *testing.T, tr *snapshotTracker, store *fakeSnapshotStore) <-chan struct{} {
	t.Helper()
	due := timeOf(tr.state(trackedActorUID).GetUploadDueTime())
	if due.IsZero() {
		t.Fatal("no upload scheduled")
	}
	stepTo(tr, due)
	waitStarted(t, store)
	return opDone(t, tr, trackedActorUID)
}

// uploadNow runs the tracked actor's upload once it is due, and ends it on the
// blocking store with err.
func uploadNow(t *testing.T, tr *snapshotTracker, store *fakeSnapshotStore, err error) {
	t.Helper()
	done := startUpload(t, tr, store)
	store.release <- err
	waitClosed(t, done, "the upload")
}

// detachDueOf returns when atelet detaches the actor, or zero if it does not.
func detachDueOf(tr *snapshotTracker, actorUID string) time.Time {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if a := tr.actors[actorUID]; a != nil {
		due, _ := a.detachDue()
		return due
	}
	return time.Time{}
}

// detachNow advances the clock to when the tracked actor's detach is due, and
// waits for a worker to detach it.
func detachNow(t *testing.T, tr *snapshotTracker) {
	t.Helper()
	due := detachDueOf(tr, trackedActorUID)
	if due.IsZero() {
		t.Fatal("no detach scheduled")
	}
	stepTo(tr, due)
	eventually(t, "the actor is detached", func() bool {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		a := tr.actors[trackedActorUID]
		return a.op == nil && a.latestLocal == nil
	})
}

// processNext takes the next actor from the queue of a tracker whose workers
// have not started, and runs its background work as a worker would.
func processNext(t *testing.T, tr *snapshotTracker) {
	t.Helper()
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := tr.queue.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.syncActor(actorUID)
		tr.queue.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an actor to be queued")
	}
}

func timeOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func uuidOf(r *ateletpb.SnapshotReport) string {
	return r.GetSnapshot().GetSnapshotUuid()
}

// Deferred checkpoints keep their snapshots local, and atelet uploads the
// latest one once the oldest snapshot not yet durable has waited the upload
// delay. The local copy stays after the upload.
func TestTrackerUploadsLatestLocalSnapshot(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()

	keep, st := tr.commitLocal(trackedActorUID, testRef("s1", 1, t0))
	if !slices.Equal(keep, []string{"s1"}) {
		t.Errorf("keep = %v, want [s1]", keep)
	}
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}

	_, st = tr.commitLocal(trackedActorUID, testRef("s2", 2, t0.Add(10*time.Minute)))
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: when the first snapshot was due", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got, want := store.uploads(), []uploadCall{{"s2", noFidelity}}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
	st = tr.state(trackedActorUID)
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want s2 with its upload time", got)
	}
	if got := uuidOf(st.GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local = %q, want s2 kept after its upload", got)
	}
	if st.GetUploading() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want no upload in progress or due", st)
	}
}

// A deferred checkpoint during an older snapshot's upload keeps that snapshot
// until the upload ends. The upload's end deletes it and schedules the new
// snapshot's upload, which the older upload does not cover.
func TestTrackerCheckpointDuringUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	for _, name := range []string{"s1", "s2"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t0 := tr.clock.Now()
	tr.commitLocal(trackedActorUID, testRef("s1", 1, t0))
	done := startUpload(t, tr, store)

	s2Time := t0.Add(10 * time.Minute)
	keep, st := tr.commitLocal(trackedActorUID, testRef("s2", 2, s2Time))
	if !slices.Equal(keep, []string{"s2", "s1"}) {
		t.Errorf("keep = %v, want [s2 s1]: s1 is being uploaded", keep)
	}
	if got := uuidOf(st.GetUploading()); got != "s1" {
		t.Errorf("uploading = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), s2Time.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}

	store.release <- nil
	waitClosed(t, done, "the upload of s1")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after its upload: stat = %v, want it deleted", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s2")); err != nil {
		t.Errorf("s2 after the upload of s1: %v, want it kept", err)
	}
	st = tr.state(trackedActorUID)
	if got := uuidOf(st.GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), s2Time.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.state(trackedActorUID).GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q, want s2", got)
	}
}

// An immediate checkpoint makes the actor's newest snapshot durable. It drops
// the local snapshot and the scheduled detach. It cancels an older upload,
// which does not count as a failure, and returns once that upload has ended
// and deleted the older snapshot's local copy.
func TestTrackerImmediateCheckpointCancelsUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	t0 := tr.clock.Now()
	tr.commitLocal(trackedActorUID, testRef("s1", 1, t0))
	startUpload(t, tr, store)

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.uploadTime = s2.commitTime.Add(time.Second)
	st := tr.commitDurable(t.Context(), trackedActorUID, s2)
	if st.GetLatestLocal() != nil || st.GetUploading() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want nothing local, uploading or due", st)
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.uploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.uploadTime)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after the immediate checkpoint: stat = %v, want it deleted", err)
	}
	if !detachDueOf(tr, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled after an immediate checkpoint")
	}

	// The next deferred checkpoint is due a full delay after it was taken.
	s3Time := tr.clock.Now()
	_, st = tr.commitLocal(trackedActorUID, testRef("s3", 3, s3Time))
	if got, want := timeOf(st.GetUploadDueTime()), s3Time.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}
}

// A checkpoint repeating the newest snapshot atelet took is a retry, and one
// older than the newest snapshot it took or restored is stale.
func TestTrackerBeginCheckpoint(t *testing.T) {
	tr := newTestTracker(t, newFakeSnapshotStore())
	check := func(uuid string, generation int32, wantRetry bool, wantCode codes.Code) {
		t.Helper()
		retry, err := tr.beginCheckpoint(t.Context(), trackedActorUID, testRef(uuid, generation, time.Time{}))
		if retry != wantRetry || apierror.Code(err) != wantCode {
			t.Errorf("beginCheckpoint(%s at %d) = %v, %v; want %v, %v", uuid, generation, retry, err, wantRetry, wantCode)
		}
	}
	check("s1", 1, false, codes.OK)

	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	check("s1", 1, true, codes.OK)
	check("other", 1, false, codes.Aborted)
	check("s2", 2, false, codes.OK)

	tr.restored(trackedActorUID, testRef("s5", 5, time.Time{}))
	check("s5", 5, false, codes.OK)
	check("s4", 4, false, codes.Aborted)
	check("s6", 6, false, codes.OK)
}

// PromoteSnapshot makes the named snapshot durable while keeping its local
// copy, and checks its minimum fidelity.
func TestTrackerPromote(t *testing.T) {
	t.Run("local snapshot kept after promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		want := testRef("s1", 1, time.Time{})
		want.fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES

		st, err := tr.promote(t.Context(), trackedActorUID, want)
		if err != nil {
			t.Fatalf("promote: %v", err)
		}
		if got, wantCalls := store.uploads(), []uploadCall{{"s1", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES}}; !slices.Equal(got, wantCalls) {
			t.Errorf("uploads = %v, want %v", got, wantCalls)
		}
		if uuidOf(st.GetLatestLocal()) != "s1" || st.GetUploadDueTime() != nil {
			t.Errorf("state = %v, want s1 kept locally and no upload due", st)
		}
		durable := st.GetLatestDurable()
		if got := durable.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
			t.Errorf("latest_durable fidelity = %v, want the MEMORY it holds", got)
		}
		if durable.GetDurableCompleteTime() == nil || durable.GetLocalCommitTime() == nil {
			t.Errorf("latest_durable = %v, want its commit and upload times", durable)
		}
		if detachDueOf(tr, trackedActorUID).IsZero() {
			t.Error("no detach scheduled after promote, want it kept")
		}
	})

	t.Run("untracked, as after a restart", func(t *testing.T) {
		tr := newTestTracker(t, newFakeSnapshotStore())
		want := testRef("s2", 2, time.Time{})
		st, err := tr.promote(t.Context(), trackedActorUID, want)
		if err != nil {
			t.Fatalf("promote: %v", err)
		}
		if got := uuidOf(st.GetLatestDurable()); got != "s2" {
			t.Errorf("latest_durable = %q, want s2", got)
		}
		// Requests from a view older than the promoted snapshot are stale.
		stale := testRef("s1", 1, time.Time{})
		if _, err := tr.promote(t.Context(), trackedActorUID, stale); apierror.Code(err) != codes.Aborted {
			t.Errorf("stale promote = %v, want Aborted", err)
		}
		if _, err := tr.beginCheckpoint(t.Context(), trackedActorUID, stale); apierror.Code(err) != codes.Aborted {
			t.Errorf("stale checkpoint = %v, want Aborted", err)
		}
	})

	t.Run("running actor rejected", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		if _, err := tr.claimRunning(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("claimRunning: %v", err)
		}
		if _, err := tr.promote(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{})); apierror.Code(err) != codes.Aborted {
			t.Errorf("promote of a running actor = %v, want Aborted", err)
		}
	})
}

// A PromoteSnapshot for a newer snapshot than the one being uploaded cancels
// that upload, whose end deletes the superseded snapshot's local copy, and
// then uploads the newer one.
func TestTrackerPromoteCancelsOlderUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	for _, name := range []string{"s1", "s2"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t0 := tr.clock.Now()
	tr.commitLocal(trackedActorUID, testRef("s1", 1, t0))
	stepTo(tr, t0.Add(testUploadDelay))
	// Upload s1 as a worker would, with none running to take the actor
	// once the upload ends.
	go tr.syncActor(trackedActorUID)
	if got := waitStarted(t, store); got != "s1" {
		t.Fatalf("started = %q, want s1", got)
	}
	tr.commitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

	store.setBlocking(false)
	st, err := tr.promote(t.Context(), trackedActorUID, testRef("s2", 2, time.Time{}))
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	wantCalls := []uploadCall{{"s1", noFidelity}, {"s2", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}}
	if got := store.uploads(); !slices.Equal(got, wantCalls) {
		t.Errorf("uploads = %v, want %v", got, wantCalls)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after the promote of s2: stat = %v, want it deleted", err)
	}
	if uuidOf(st.GetLatestLocal()) != "s2" || uuidOf(st.GetLatestDurable()) != "s2" || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want s2 local and durable, and no upload due", st)
	}
}

// A detach makes the latest local snapshot durable and drops it, and is a
// no-op when atelet holds no local snapshot for the actor.
func TestTrackerDetach(t *testing.T) {
	t.Run("requested", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		st, err := tr.detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got, wantCalls := store.uploads(), []uploadCall{{"s1", noFidelity}}; !slices.Equal(got, wantCalls) {
			t.Errorf("uploads = %v, want %v", got, wantCalls)
		}
		if st.GetLatestLocal() != nil || st.GetUploadDueTime() != nil {
			t.Errorf("state = %v, want no local snapshot and no upload due", st)
		}
		durable := st.GetLatestDurable()
		if got := durable.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
			t.Errorf("latest_durable fidelity = %v, want the MEMORY it holds", got)
		}
		if durable.GetDurableCompleteTime() == nil || durable.GetLocalCommitTime() == nil {
			t.Errorf("latest_durable = %v, want its commit and upload times", durable)
		}
		if !detachDueOf(tr, trackedActorUID).IsZero() {
			t.Error("a detach is scheduled after a detach")
		}
	})

	t.Run("no-op when no local snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		st, err := tr.detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach of untracked actor: %v", err)
		}
		if !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("state = %v, want empty", st)
		}
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none", got)
		}
	})

	t.Run("by the timer", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		// By the time the detach is due, so is the upload: the detach runs,
		// and makes s1 durable itself.
		detachNow(t, tr)
		if got, want := store.uploads(), []uploadCall{{"s1", noFidelity}}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.state(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
	})

	t.Run("by the timer after the upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		uploadTime := tr.state(trackedActorUID).GetLatestDurable().GetDurableCompleteTime()

		detachNow(t, tr)
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the background upload's", got)
		}
		st := tr.state(trackedActorUID)
		if st.GetLatestLocal() != nil || !proto.Equal(st.GetLatestDurable().GetDurableCompleteTime(), uploadTime) {
			t.Errorf("state = %v, want nothing local and s1 durable since %v", st, uploadTime)
		}
	})

	t.Run("waits for the upload of its snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		startUpload(t, tr, store)
		detached := make(chan error, 1)
		go func() {
			_, err := tr.detach(t.Context(), trackedActorUID)
			detached <- err
		}()

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the in-progress upload's", got)
		}
		if got := tr.state(trackedActorUID).GetLatestDurable(); got.GetDurableCompleteTime() == nil {
			t.Errorf("latest_durable = %v, want the background upload's time", got)
		}
	})
}

// atelet does not detach an actor that runs on the node, and schedules the
// detach again only once the actor is suspended here again.
func TestTrackerDetachTimer(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	release, err := tr.claimRunning(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("claimRunning: %v", err)
	}
	if !detachDueOf(tr, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled while the actor runs")
	}
	if _, err := tr.detach(t.Context(), trackedActorUID); apierror.Code(err) != codes.Aborted {
		t.Errorf("detach of a running actor = %v, want Aborted", err)
	}
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v, want none for a running actor", got)
	}

	// A start that fails leaves the actor suspended on the node.
	release(false)
	if got, want := detachDueOf(tr, trackedActorUID), tr.clock.Now().Add(testDetachDelay); !got.Equal(want) {
		t.Fatalf("detach due at %v after a failed start, want %v", got, want)
	}
	detachNow(t, tr)

	tr.commitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	release, err = tr.claimRunning(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("claimRunning: %v", err)
	}
	release(true)
	if !detachDueOf(tr, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled after the actor started")
	}
	tr.commitLocal(trackedActorUID, testRef("s3", 3, tr.clock.Now()))
	if detachDueOf(tr, trackedActorUID).IsZero() {
		t.Error("no detach scheduled after the next deferred checkpoint")
	}
}

// A restore or a checkpoint that arrives during a detach waits for it to end.
func TestTrackerWaitsForDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	results := map[string]chan error{
		"detach":          make(chan error, 1),
		"claimRunning":    make(chan error, 1),
		"beginCheckpoint": make(chan error, 1),
	}
	go func() {
		_, err := tr.detach(t.Context(), trackedActorUID)
		results["detach"] <- err
	}()
	waitStarted(t, store)

	go func() {
		_, err := tr.claimRunning(t.Context(), trackedActorUID)
		results["claimRunning"] <- err
	}()
	go func() {
		_, err := tr.beginCheckpoint(t.Context(), trackedActorUID, testRef("s2", 2, time.Time{}))
		results["beginCheckpoint"] <- err
	}()
	select {
	case err := <-results["claimRunning"]:
		t.Fatalf("claimRunning returned (%v) during a detach", err)
	case err := <-results["beginCheckpoint"]:
		t.Fatalf("beginCheckpoint returned (%v) during a detach", err)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- nil
	for name, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not return after the detach", name)
		}
	}
	if got := tr.state(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v after the detach, want unset", got)
	}
}

// A failed background upload is retried once the retry delay has passed, and
// the delay doubles with each further failure.
func TestTrackerRetriesFailedUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	for _, delay := range []time.Duration{testRetryDelay, 2 * testRetryDelay} {
		uploadNow(t, tr, store, errors.New("boom"))
		st := tr.state(trackedActorUID)
		if st.GetLatestDurable() != nil {
			t.Errorf("latest_durable = %v after a failed upload, want unset", st.GetLatestDurable())
		}
		if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(delay); !got.Equal(want) {
			t.Errorf("upload_due_time = %v, want %v: %v after the failure", got, want, delay)
		}
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.state(trackedActorUID).GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q after the retry, want s1", got)
	}
}

// A failed detach keeps the local snapshot, and is retried, as is the upload,
// no sooner than the retry delay after it.
func TestTrackerFailedDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setErr(errors.New("boom"))
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	if _, err := tr.detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the store's error")
	}
	retryAt := tr.clock.Now().Add(testRetryDelay)
	st := tr.state(trackedActorUID)
	if uuidOf(st.GetLatestLocal()) != "s1" || timeOf(st.GetUploadDueTime()).Before(retryAt) {
		t.Errorf("state = %v, want s1 kept and its upload due no sooner than %v", st, retryAt)
	}
	if got := detachDueOf(tr, trackedActorUID); !got.Equal(retryAt) {
		t.Errorf("detach due at %v after the failed one, want %v", got, retryAt)
	}

	store.setErr(nil)
	detachNow(t, tr)
	if got := tr.state(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v after the retried detach, want unset", got)
	}
}

// forget cancels an upload in progress and waits for it to end.
func TestTrackerForgetCancelsUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	done := startUpload(t, tr, store)

	if err := tr.forget(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	select {
	case <-done:
	default:
		t.Error("forget returned before the upload ended")
	}
	if st := tr.state(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", st)
	}
	if !detachDueOf(tr, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled after forget")
	}
}

// The workers bound how many background uploads run at once, and an actor
// that checkpoints again while it waits for a worker uploads only its newest
// snapshot.
func TestTrackerWorkersBoundBackgroundWork(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	tr.workers = 2
	tr.start()

	// Snapshots taken an upload delay ago are due for upload straight away.
	taken := tr.clock.Now().Add(-testUploadDelay)
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		tr.commitLocal(id, testRef(id+"-s1", 1, taken))
	}
	started := []string{waitStarted(t, store), waitStarted(t, store)}
	slices.Sort(started)
	if want := []string{"actor-1-s1", "actor-2-s1"}; !slices.Equal(started, want) {
		t.Fatalf("first started = %v, want %v", started, want)
	}
	select {
	case got := <-store.started:
		t.Fatalf("a third upload (%q) started while both workers are busy", got)
	default:
	}
	if st := tr.state("actor-3"); st.GetUploading() != nil || st.GetUploadDueTime() == nil {
		t.Errorf("actor-3 state while it waits = %v, want an upload due and none in progress", st)
	}

	// actor-3 checkpoints again while it waits for a worker.
	tr.commitLocal("actor-3", testRef("actor-3-s2", 2, taken.Add(time.Minute)))

	done1 := opDone(t, tr, "actor-1")
	done2 := opDone(t, tr, "actor-2")
	// Ending one upload frees a worker, which takes actor-3.
	store.release <- nil
	if got := waitStarted(t, store); got != "actor-3-s2" {
		t.Fatalf("started = %q, want actor-3-s2", got)
	}
	done3 := opDone(t, tr, "actor-3")
	store.release <- nil
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	waitClosed(t, done2, "actor-2's upload")
	waitClosed(t, done3, "actor-3's upload")

	if got := uuidOf(tr.state("actor-3").GetLatestDurable()); got != "actor-3-s2" {
		t.Errorf("actor-3 latest_durable = %q, want actor-3-s2", got)
	}
	if got := len(store.uploads()); got != 3 {
		t.Errorf("uploads = %v, want 3: actor-3-s1 superseded before it was uploaded", store.uploads())
	}
}

// DetachActor runs in the calling goroutine, so it does not wait behind other
// actors' background work, and a worker that takes the actor afterwards finds
// nothing left to do.
func TestTrackerForegroundDetachBypassesQueue(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	tr.workers = 1
	tr.start()

	taken := tr.clock.Now().Add(-testUploadDelay)
	tr.commitLocal("actor-1", testRef("a1-s1", 1, taken))
	if got := waitStarted(t, store); got != "a1-s1" {
		t.Fatalf("started = %q, want a1-s1", got)
	}
	// actor-2's upload is due too, but the only worker is busy.
	tr.commitLocal("actor-2", testRef("a2-s1", 1, taken))

	store.setBlocking(false)
	st, err := tr.detach(t.Context(), "actor-2")
	if err != nil {
		t.Fatalf("detach of actor-2: %v", err)
	}
	if got := waitStarted(t, store); got != "a2-s1" {
		t.Fatalf("started = %q, want a2-s1", got)
	}
	if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "a2-s1" {
		t.Errorf("actor-2 state = %v, want detached with a2-s1 durable", st)
	}

	done1 := opDone(t, tr, "actor-1")
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	// actor-3 is queued behind actor-2, so its upload starts only once the
	// worker has taken actor-2.
	tr.commitLocal("actor-3", testRef("a3-s1", 1, taken))
	if got := waitStarted(t, store); got != "a3-s1" {
		t.Fatalf("started = %q, want a3-s1", got)
	}
	wantCalls := []uploadCall{{"a1-s1", noFidelity}, {"a2-s1", noFidelity}, {"a3-s1", noFidelity}}
	if got := store.uploads(); !slices.Equal(got, wantCalls) {
		t.Errorf("uploads = %v, want %v", got, wantCalls)
	}
}

// The queue holds an actor until the earliest time it was queued for. A worker
// that takes the actor before any of its work is due, as after its upload was
// no longer needed, does nothing and queues it again for when its work is.
func TestTrackerRequeuesUntilWorkIsDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	// Queued for the upload, due an hour out. The promote makes it durable,
	// which leaves the detach, due later.
	tr.commitLocal(trackedActorUID, testRef("s1", 1, t0))
	if _, err := tr.promote(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{})); err != nil {
		t.Fatalf("promote: %v", err)
	}

	stepTo(tr, t0.Add(testUploadDelay))
	processNext(t, tr)
	if got := len(store.uploads()); got != 1 {
		t.Errorf("%d store calls, want only the promote's", got)
	}
	if got := uuidOf(tr.state(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q before the detach is due, want s1", got)
	}

	stepTo(tr, t0.Add(testDetachDelay))
	processNext(t, tr)
	if got := tr.state(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v once the detach was due, want unset", got)
	}
}
