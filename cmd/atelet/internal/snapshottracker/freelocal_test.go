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

func TestFreeLocal(t *testing.T) {
	tests := []struct {
		name   string
		actors []localActor
		target int64
		want   []string // the actors whose detach FreeLocal makes due now
	}{
		{
			name: "oldest checkpoint first, stopping once the target is covered",
			actors: []localActor{
				{uid: "b", age: 20 * time.Minute, durable: true},
				{uid: "a", age: 30 * time.Minute, durable: true},
				{uid: "c", age: 10 * time.Minute, durable: true},
			},
			target: snapshotSize + snapshotSize/2,
			want:   []string{"a", "b"},
		},
		{
			name: "an undurable snapshot is left out",
			actors: []localActor{
				{uid: "a", age: 30 * time.Minute},
				{uid: "b", age: 20 * time.Minute, durable: true},
			},
			target: 10 * snapshotSize,
			want:   []string{"b"},
		},
		{
			name:   "a snapshot younger than the minimum age is left out",
			actors: []localActor{{uid: "a", age: freeLocalMinAge - time.Second, durable: true}},
			target: 10 * snapshotSize,
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
			now := tr.clock.Now()
			var dueNow []string
			for _, a := range tc.actors {
				if detachDueOf(tr, a.uid).Equal(now) {
					dueNow = append(dueNow, a.uid)
				}
			}
			slices.Sort(dueNow)
			if !slices.Equal(dueNow, tc.want) {
				t.Errorf("detaches due now = %v, want %v", dueNow, tc.want)
			}
			if least := int64(len(tc.want)) * snapshotSize; got < least {
				t.Errorf("FreeLocal scheduled %d bytes, want at least %d", got, least)
			}
		})
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
