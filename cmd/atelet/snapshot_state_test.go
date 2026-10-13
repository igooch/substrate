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
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// restartTracker returns a tracker over store loaded from the snapshot state
// files on the node, with its workers running, as a restarted atelet builds.
func restartTracker(t *testing.T, store snapshotStore) *snapshotTracker {
	t.Helper()
	tr := newTestTrackerOnNode(t, store)
	if err := tr.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	tr.start()
	return tr
}

// writeStateFile writes rec as the actor's snapshot state file.
func writeStateFile(t *testing.T, actorUID string, rec *actorSnapshotsRecord) {
	t.Helper()
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ateletpath.ActorSnapshotStateFile(actorUID), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A restarted atelet carries on from the state file: the same state, a retried
// checkpoint still recognized and a stale one refused, and the detach due when
// it was.
func TestSnapshotStateSurvivesRestart(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	before := tr.state(trackedActorUID)
	detachDue := detachDueOf(tr, trackedActorUID)

	restarted := restartTracker(t, store)
	if got := restarted.state(trackedActorUID); !proto.Equal(got, before) {
		t.Errorf("state after the restart = %v, want %v", got, before)
	}
	if retry, err := restarted.beginCheckpoint(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{})); !retry || err != nil {
		t.Errorf("beginCheckpoint for the committed snapshot = %v, %v; want a retry", retry, err)
	}
	if _, err := restarted.beginCheckpoint(t.Context(), trackedActorUID, testRef("other", 1, time.Time{})); apierror.Code(err) != codes.Aborted {
		t.Errorf("beginCheckpoint from a stale view = %v, want Aborted", err)
	}
	if got := detachDueOf(restarted, trackedActorUID); !got.Equal(detachDue) {
		t.Errorf("detach due at %v after the restart, want %v", got, detachDue)
	}
	detachNow(t, restarted)
	if got := restarted.state(trackedActorUID); got.GetLatestLocal() != nil || uuidOf(got.GetLatestDurable()) != "s1" {
		t.Errorf("state after the detach = %v, want s1 durable and nothing local", got)
	}
}

// An upload in flight at a restart is due again: its snapshot was not made
// durable, and counts from when it was taken.
func TestSnapshotStateRestartEndsUpload(t *testing.T) {
	useTempNodeDirs(t)
	t0 := time.Now().Add(-10 * time.Minute)
	t1 := t0.Add(5 * time.Minute)
	writeStateFile(t, trackedActorUID, &actorSnapshotsRecord{
		Newest:             recordOf(&snapshotRef{uuid: "s2", generation: 2}),
		NewestByCheckpoint: true,
		LatestLocal:        recordOf(&snapshotRef{uuid: "s2", generation: 2, commitTime: t1}),
		Uploading:          recordOf(&snapshotRef{uuid: "s1", generation: 1, commitTime: t0}),
		UndurableSince:     t1,
		DetachDue:          t1.Add(2 * time.Hour),
	})

	tr := restartTracker(t, newFakeSnapshotStore())
	st := tr.state(trackedActorUID)
	if st.GetUploading() != nil {
		t.Errorf("uploading = %v after the restart, want unset", st.GetUploading())
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(time.Hour); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an hour after the interrupted upload's snapshot", got, want)
	}
}

// A restarted atelet does not detach an actor that was running.
func TestSnapshotStateKeepsRunning(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	if _, err := tr.claimRunning(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("claimRunning: %v", err)
	}

	restarted := restartTracker(t, store)
	if !detachDueOf(restarted, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled for a running actor after the restart")
	}
	if _, err := restarted.detach(t.Context(), trackedActorUID); apierror.Code(err) != codes.Aborted {
		t.Errorf("detach of a running actor after the restart = %v, want Aborted", err)
	}
}

// A detach is remembered across a restart: the state still reports the
// detached snapshot as durable, and nothing is scheduled.
func TestSnapshotStateAfterDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	before, err := tr.detach(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("detach: %v", err)
	}

	restarted := restartTracker(t, store)
	if got := restarted.state(trackedActorUID); !proto.Equal(got, before) {
		t.Errorf("state after the restart = %v, want %v", got, before)
	}
	if !detachDueOf(restarted, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled for a detached actor after the restart")
	}
}

// forget deletes the actor's state file, so a restart knows nothing of it.
func TestForgetDeletesSnapshotState(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); err != nil {
		t.Fatalf("state file after a checkpoint: %v", err)
	}

	if err := tr.forget(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forget: stat = %v, want it deleted", err)
	}
	if got := restartTracker(t, store).state(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after the restart = %v, want empty", got)
	}
}

// A state file that does not parse is skipped, and the others still load.
func TestLoadSkipsUnreadableSnapshotState(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	const brokenUID = "actor-uid-broken"
	if err := os.MkdirAll(ateletpath.ActorPath(brokenUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ateletpath.ActorSnapshotStateFile(brokenUID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted := restartTracker(t, store)
	if got := uuidOf(restarted.state(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q after the restart, want s1", got)
	}
	if got := restarted.state(brokenUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state of the actor with a broken file = %v, want empty", got)
	}
}

// atelet writes no state for an actor without a directory, and does not
// remember an actor it knew nothing of when a PromoteSnapshot for it fails.
func TestSnapshotStateNeedsActorDir(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	store.setErr(errors.New("boom"))
	tr := newTestTrackerOnNode(t, store)
	want := testRef("s1", 1, time.Time{})
	if _, err := tr.promote(t.Context(), trackedActorUID, want); err == nil {
		t.Fatal("promote succeeded, want the store's error")
	}
	tr.mu.Lock()
	_, tracked := tr.actors[trackedActorUID]
	tr.mu.Unlock()
	if tracked {
		t.Error("the actor is tracked after a failed promote, want it unknown")
	}

	tr.commitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir: stat = %v, want none created for the state file", err)
	}
}
