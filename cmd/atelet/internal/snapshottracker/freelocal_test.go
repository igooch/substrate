// Copyright 2026 The Agent Substrate Authors
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

package snapshottracker

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

const snapshotSize = 1 << 20

// localActor is a suspended actor for FreeLocal tests.
type localActor struct {
	uid     string
	age     time.Duration // how long before now its checkpoint committed
	durable bool          // its local snapshot is already in object storage
	started bool          // a Run or Restore began after the checkpoint
}

// addLocalActor gives the actor a directory and a local snapshot of
// snapshotSize bytes, commits it as the actor's latest local snapshot, and,
// for a durable one, as its latest durable snapshot too, as an immediate
// checkpoint does.
func addLocalActor(t *testing.T, tr *Tracker, a localActor) {
	t.Helper()
	uuid := a.uid + "-s1"
	dir := filepath.Join(ateletpath.ActorSnapshotPath(a.uid), uuid)
	if err := os.MkdirAll(ateletpath.ActorPath(a.uid), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data"), make([]byte, snapshotSize), 0o600); err != nil {
		t.Fatal(err)
	}
	ref := testRef(uuid, 1, tr.clock.Now().Add(-a.age))
	if a.durable {
		tr.CommitDurable(a.uid, ref)
	}
	tr.CommitLocal(a.uid, ref)
	if a.started {
		end, err := tr.BeginStart(t.Context(), a.uid)
		if err != nil {
			t.Fatal(err)
		}
		end()
	}
}

// uploadDueOf returns when atelet uploads the actor's snapshot, or zero if it
// does not.
func uploadDueOf(tr *Tracker, actorUID string) time.Time {
	tr.lock(actorUID)
	defer tr.unlock(actorUID)
	if a := tr.findLocked(actorUID); a != nil {
		due, _ := tr.uploadDueLocked(a)
		return due
	}
	return time.Time{}
}

// dueNow returns, sorted, the actors whose detach, or upload, is due now.
func dueNow(tr *Tracker, actors []localActor, due func(*Tracker, string) time.Time) []string {
	var out []string
	for _, a := range actors {
		if due(tr, a.uid).Equal(tr.clock.Now()) {
			out = append(out, a.uid)
		}
	}
	slices.Sort(out)
	return out
}

func TestFreeLocal(t *testing.T) {
	tests := []struct {
		name        string
		actors      []localActor
		target      int64
		wantDetach  []string // the actors whose detach FreeLocal makes due now
		wantUploads []string // the actors whose upload FreeLocal makes due now
	}{
		{
			name: "oldest checkpoint first, stopping once the target is covered",
			actors: []localActor{
				{uid: "b", age: 20 * time.Minute, durable: true},
				{uid: "a", age: 30 * time.Minute, durable: true},
				{uid: "c", age: 10 * time.Minute, durable: true},
			},
			target:     snapshotSize + snapshotSize/2,
			wantDetach: []string{"a", "b"},
		},
		{
			name:       "a durable snapshot committed just now qualifies",
			actors:     []localActor{{uid: "a", durable: true}},
			target:     snapshotSize,
			wantDetach: []string{"a"},
		},
		{
			name: "an undurable snapshot is never detached, and its upload starts early when durable ones fall short",
			actors: []localActor{
				{uid: "a", age: 30 * time.Minute},
				{uid: "b", age: 20 * time.Minute, durable: true},
			},
			target:      snapshotSize + snapshotSize/2,
			wantDetach:  []string{"b"},
			wantUploads: []string{"a"},
		},
		{
			name: "undurable snapshots are left alone when durable ones cover the target",
			actors: []localActor{
				{uid: "a", age: 30 * time.Minute},
				{uid: "b", age: 20 * time.Minute, durable: true},
			},
			target:     snapshotSize / 2,
			wantDetach: []string{"b"},
		},
		{
			name: "early uploads go oldest first, only as many as the target needs",
			actors: []localActor{
				{uid: "c", age: 10 * time.Minute},
				{uid: "a", age: 30 * time.Minute},
				{uid: "b", age: 20 * time.Minute},
			},
			target:      snapshotSize + snapshotSize/2,
			wantUploads: []string{"a", "b"},
		},
		{
			name:   "an actor started since its checkpoint is left out",
			actors: []localActor{{uid: "a", age: 30 * time.Minute, durable: true, started: true}},
			target: 10 * snapshotSize,
		},
		{
			name:   "no target frees nothing",
			actors: []localActor{{uid: "a", age: 30 * time.Minute, durable: true}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			for _, a := range tc.actors {
				addLocalActor(t, tr, a)
			}
			got, err := tr.FreeLocal(t.Context(), tc.target)
			if err != nil {
				t.Fatalf("FreeLocal: %v", err)
			}
			if d := dueNow(tr, tc.actors, detachDueOf); !slices.Equal(d, tc.wantDetach) {
				t.Errorf("detaches due now = %v, want %v", d, tc.wantDetach)
			}
			if u := dueNow(tr, tc.actors, uploadDueOf); !slices.Equal(u, tc.wantUploads) {
				t.Errorf("uploads due now = %v, want %v", u, tc.wantUploads)
			}
			if least := int64(len(tc.wantDetach)) * snapshotSize; got < least {
				t.Errorf("FreeLocal scheduled %d bytes, want at least %d", got, least)
			}
		})
	}
}

// Uploads FreeLocal already made due count toward the target, so a second
// call doesn't pull another one forward.
func TestFreeLocalCountsEarlyUploads(t *testing.T) {
	tr := newIdleTestTracker(t, newFakeSnapshotStore())
	actors := []localActor{
		{uid: "a", age: 30 * time.Minute},
		{uid: "b", age: 20 * time.Minute},
		{uid: "c", age: 10 * time.Minute},
	}
	for _, a := range actors {
		addLocalActor(t, tr, a)
	}
	for range 2 {
		if _, err := tr.FreeLocal(t.Context(), snapshotSize+snapshotSize/2); err != nil {
			t.Fatalf("FreeLocal: %v", err)
		}
	}
	if u, want := dueNow(tr, actors, uploadDueOf), []string{"a", "b"}; !slices.Equal(u, want) {
		t.Errorf("uploads due now after two calls = %v, want %v", u, want)
	}
}

// FreeLocal saves the earlier detach, so an atelet restart keeps it, and a
// detach worker then deletes the snapshot.
func TestFreeLocalDetachSurvivesRestartAndRuns(t *testing.T) {
	tr := newIdleTestTracker(t, newFakeSnapshotStore())
	addLocalActor(t, tr, localActor{uid: "a", age: 30 * time.Minute, durable: true})
	if _, err := tr.FreeLocal(t.Context(), snapshotSize); err != nil {
		t.Fatalf("FreeLocal: %v", err)
	}
	if due := detachDueOf(newTracker(t), "a"); !due.Equal(tr.clock.Now()) {
		t.Errorf("detachDue read back from the state file = %v, want now", due)
	}
	processNext(t, tr, opDetach)
	if got := tr.State("a").GetLatestLocal(); got != nil {
		t.Errorf("latest_local after the detach worker ran = %v, want unset", got)
	}
}

// A Run or Restore that begins after FreeLocal cancels the early detach, as it
// does a timed one.
func TestFreeLocalDetachCanceledByStart(t *testing.T) {
	tr := newIdleTestTracker(t, newFakeSnapshotStore())
	addLocalActor(t, tr, localActor{uid: "a", age: 30 * time.Minute, durable: true})
	if _, err := tr.FreeLocal(t.Context(), snapshotSize); err != nil {
		t.Fatalf("FreeLocal: %v", err)
	}
	end, err := tr.BeginStart(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	end()
	if due := detachDueOf(tr, "a"); !due.IsZero() {
		t.Errorf("detachDue after a start = %v, want none", due)
	}
}
