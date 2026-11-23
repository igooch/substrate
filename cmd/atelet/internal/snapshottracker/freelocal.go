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
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// freeLocalMinAge keeps a durable local snapshot from an early detach for this
// long after its checkpoint, so an actor that suspends and resumes soon after
// still resumes from this node. It matters for immediate checkpoints, whose
// snapshot is durable at once. A deferred one is durable only after
// DefaultUploadDelay anyway.
const freeLocalMinAge = 5 * time.Minute

// FreeLocal brings forward the detach of suspended actors whose latest local
// snapshot is already durable, oldest checkpoint first, until their local
// snapshots add up to target bytes, and returns the bytes it scheduled. The
// detach workers then delete the snapshots and report each detach, as they do
// once the detach delay passes. A tracker without a store detaches nothing.
func (t *Tracker) FreeLocal(ctx context.Context, target int64) (int64, error) {
	if t.store == nil || target <= 0 {
		return 0, nil
	}
	candidates, err := t.earlyDetachCandidates(ctx)
	if err != nil {
		return 0, err
	}
	var scheduled int64
	for _, actorUID := range candidates {
		if scheduled >= target {
			break
		}
		// Sized outside the actor's lock, since it walks the actor's files.
		size := localSnapshotBytes(actorUID)
		ok, err := t.detachEarly(ctx, actorUID)
		if err != nil {
			return scheduled, err
		}
		if ok {
			slog.InfoContext(ctx, "Detaching a durable local snapshot early to free disk",
				slog.String("actorUID", actorUID), slog.Int64("bytes", size))
			scheduled += size
		}
	}
	return scheduled, nil
}

// earlyDetachCandidates returns the actors FreeLocal may detach early, oldest
// checkpoint first.
func (t *Tracker) earlyDetachCandidates(ctx context.Context) ([]string, error) {
	actorUIDs, err := savedActorUIDs()
	if err != nil {
		return nil, err
	}
	type candidate struct {
		actorUID  string
		committed time.Time
	}
	var candidates []candidate
	for _, actorUID := range actorUIDs {
		if !t.locks.Lock(ctx, actorUID) {
			return nil, ctx.Err()
		}
		if a := t.findLocked(actorUID); a != nil && t.earlyDetachableLocked(a) {
			candidates = append(candidates, candidate{actorUID, a.LatestLocal.CommitTime})
		}
		t.unlock(actorUID)
	}
	slices.SortFunc(candidates, func(x, y candidate) int { return x.committed.Compare(y.committed) })
	ordered := make([]string, len(candidates))
	for i, c := range candidates {
		ordered[i] = c.actorUID
	}
	return ordered, nil
}

// detachEarly makes the actor's detach due now, if it still qualifies, and
// reports whether it did. The actor may have changed since it was listed.
func (t *Tracker) detachEarly(ctx context.Context, actorUID string) (bool, error) {
	if !t.locks.Lock(ctx, actorUID) {
		return false, ctx.Err()
	}
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !t.earlyDetachableLocked(a) {
		return false, nil
	}
	a.DetachAt = t.clock.Now()
	t.saveLocked(a)
	t.requeueLocked(a)
	return true, nil
}

// earlyDetachableLocked reports whether FreeLocal may bring the actor's detach
// forward: a detach is scheduled for later, nothing runs on the actor, its
// latest local snapshot is durable already, and that snapshot is past
// freeLocalMinAge.
func (t *Tracker) earlyDetachableLocked(a *actorSnapshots) bool {
	now := t.clock.Now()
	if !a.detachScheduled() || !a.DetachAt.After(now) {
		return false
	}
	if a.op != nil || a.hold != holdNone || a.retryAt.After(now) {
		return false
	}
	if now.Sub(a.LatestLocal.CommitTime) < freeLocalMinAge {
		return false
	}
	_, durable, err := a.detachTarget()
	return err == nil && durable
}

// localSnapshotBytes returns the bytes deleting the actor's local snapshots
// would free: allocated blocks, leaving out files with other links, such as
// restored files linked from the snapshot cache, since deleting a link frees
// nothing. Entries it can't read count as nothing.
func localSnapshotBytes(actorUID string) int64 {
	var total int64
	_ = filepath.WalkDir(ateletpath.ActorSnapshotPath(actorUID), func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		var st unix.Stat_t
		if unix.Lstat(path, &st) != nil {
			return nil
		}
		if st.Mode&unix.S_IFMT != unix.S_IFDIR && st.Nlink > 1 {
			return nil
		}
		total += st.Blocks * 512
		return nil
	})
	return total
}
