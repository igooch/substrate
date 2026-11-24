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

// FreeLocal frees up to target bytes of local snapshots. It brings forward
// the detach of suspended actors whose latest local snapshot is already
// durable, oldest checkpoint first, and returns the bytes it scheduled. The
// detach workers then delete the snapshots and report each detach, as they do
// once the detach delay passes.
//
// When those don't cover the target, it also makes the uploads of the oldest
// undurable snapshots due now, so a later call can detach them once they are
// durable. Uploads it made due earlier and that are still running count
// toward the target, so repeated calls don't pull every upload forward. A
// tracker without a store frees nothing.
func (t *Tracker) FreeLocal(ctx context.Context, target int64) (int64, error) {
	if t.store == nil || target <= 0 {
		return 0, nil
	}
	durable, undurable, err := t.localCandidates(ctx)
	if err != nil {
		return 0, err
	}
	var scheduled int64
	for _, actorUID := range durable {
		if scheduled >= target {
			return scheduled, nil
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
	remaining := target - scheduled
	var notStarted []string
	for _, actorUID := range undurable {
		if t.uploadingEarly(actorUID) {
			remaining -= localSnapshotBytes(actorUID)
		} else {
			notStarted = append(notStarted, actorUID)
		}
	}
	for _, actorUID := range notStarted {
		if remaining <= 0 {
			break
		}
		size := localSnapshotBytes(actorUID)
		ok, err := t.uploadEarly(ctx, actorUID)
		if err != nil {
			return scheduled, err
		}
		if ok {
			slog.InfoContext(ctx, "Uploading a local snapshot early to free disk",
				slog.String("actorUID", actorUID), slog.Int64("bytes", size))
			remaining -= size
		}
	}
	return scheduled, nil
}

// localCandidates returns the suspended actors whose latest local snapshot
// FreeLocal may act on, split into those durable already and those not, each
// oldest checkpoint first.
func (t *Tracker) localCandidates(ctx context.Context) (durable, undurable []string, err error) {
	actorUIDs, err := savedActorUIDs()
	if err != nil {
		return nil, nil, err
	}
	type candidate struct {
		actorUID  string
		committed time.Time
	}
	var d, u []candidate
	for _, actorUID := range actorUIDs {
		if !t.locks.Lock(ctx, actorUID) {
			return nil, nil, ctx.Err()
		}
		if a := t.findLocked(actorUID); a != nil && t.idleSuspendedLocked(a) {
			c := candidate{actorUID, a.LatestLocal.CommitTime}
			if _, isDurable, err := a.detachTarget(); err == nil && isDurable {
				d = append(d, c)
			} else if err == nil {
				u = append(u, c)
			}
		}
		t.unlock(actorUID)
	}
	ordered := func(cs []candidate) []string {
		slices.SortFunc(cs, func(x, y candidate) int { return x.committed.Compare(y.committed) })
		out := make([]string, len(cs))
		for i, c := range cs {
			out[i] = c.actorUID
		}
		return out
	}
	return ordered(d), ordered(u), nil
}

// detachEarly makes the actor's detach due now, if its snapshot is still
// durable and nothing runs on it, and reports whether it did.
func (t *Tracker) detachEarly(ctx context.Context, actorUID string) (bool, error) {
	if !t.locks.Lock(ctx, actorUID) {
		return false, ctx.Err()
	}
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !t.idleSuspendedLocked(a) {
		return false, nil
	}
	if _, isDurable, err := a.detachTarget(); err != nil || !isDurable {
		return false, nil
	}
	a.DetachAt = t.clock.Now()
	t.saveLocked(a)
	t.requeueLocked(a)
	return true, nil
}

// uploadEarly makes the upload of the actor's undurable local snapshot due
// now, if it still waits for one and nothing runs on the actor, and reports
// whether it did.
func (t *Tracker) uploadEarly(ctx context.Context, actorUID string) (bool, error) {
	if !t.locks.Lock(ctx, actorUID) {
		return false, ctx.Err()
	}
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !t.idleSuspendedLocked(a) {
		return false, nil
	}
	if _, ok := a.uploadDue(t.uploadDelay); !ok {
		return false, nil
	}
	t.earlyMu.Lock()
	t.earlyUploads[actorUID] = t.clock.Now()
	t.earlyMu.Unlock()
	t.requeueLocked(a)
	return true, nil
}

// uploadingEarly reports whether FreeLocal made the actor's upload due early
// and the snapshot isn't durable yet.
func (t *Tracker) uploadingEarly(actorUID string) bool {
	t.earlyMu.Lock()
	defer t.earlyMu.Unlock()
	_, ok := t.earlyUploads[actorUID]
	return ok
}

// idleSuspendedLocked reports whether the actor is suspended with a detach
// scheduled for later, and nothing runs on it.
func (t *Tracker) idleSuspendedLocked(a *actorSnapshots) bool {
	now := t.clock.Now()
	if !a.detachScheduled() || !a.DetachAt.After(now) {
		return false
	}
	return a.op == nil && a.hold == holdNone && !a.retryAt.After(now)
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
