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

package main

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/nodepath"
)

// Proof of concept for #3101: free durable local snapshots when the snapshots
// filesystem passes a high watermark. The marks are constants, set by whether
// kubelet watches that filesystem.
const (
	localSnapshotGCPeriod = 30 * time.Second

	// kubeletPluginsDir is on kubelet's root filesystem, which kubelet evicts
	// pods on. atelet mounts it from the host.
	kubeletPluginsDir = "/var/lib/kubelet/plugins"

	// On kubelet's filesystem, free before kubelet's own image GC (85%).
	kubeletFSHighPct, kubeletFSLowPct = 80, 75
	// A disk kubelet doesn't watch evicts nothing. Keep room for suspends.
	otherFSHighPct, otherFSLowPct = 90, 85
)

// startLocalSnapshotGC frees local snapshots every localSnapshotGCPeriod
// while the snapshots filesystem is over its high watermark.
func startLocalSnapshotGC(ctx context.Context, tracker *snapshottracker.Tracker) {
	go func() {
		ticker := time.NewTicker(localSnapshotGCPeriod)
		defer ticker.Stop()
		for {
			freeLocalSnapshots(ctx, tracker)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func freeLocalSnapshots(ctx context.Context, tracker *snapshottracker.Tracker) {
	var st unix.Statfs_t
	if err := unix.Statfs(nodepath.SnapshotsDir, &st); err != nil {
		// No actor has written a snapshot yet.
		return
	}
	bsize := int64(st.Bsize) //nolint:unconvert // Bsize is uint32 on darwin
	total, used := int64(st.Blocks)*bsize, int64(st.Blocks-st.Bavail)*bsize
	high, low := snapshotFSWatermarks()
	target := localSnapshotDeficit(total, used, high, low)
	if target == 0 {
		return
	}
	scheduled, err := tracker.FreeLocal(ctx, target)
	attrs := []any{
		slog.Int64("total_bytes", total), slog.Int64("used_bytes", used),
		slog.Int("high_pct", high), slog.Int("low_pct", low),
		slog.Int64("target_bytes", target), slog.Int64("scheduled_bytes", scheduled),
	}
	if err != nil {
		slog.WarnContext(ctx, "Freeing local snapshots failed", append(attrs, slog.Any("err", err))...)
		return
	}
	slog.InfoContext(ctx, "Snapshots filesystem over its high watermark; detaching durable local snapshots early", attrs...)
}

// localSnapshotDeficit returns the bytes to free to bring used down to low%
// of total, once used is over high%, or 0.
func localSnapshotDeficit(total, used int64, highPct, lowPct int) int64 {
	if used*100 <= total*int64(highPct) {
		return 0
	}
	return used - total*int64(lowPct)/100
}

// snapshotFSWatermarks returns the snapshots filesystem's watermarks: the
// stricter ones when kubelet watches it, or when that can't be told.
func snapshotFSWatermarks() (highPct, lowPct int) {
	var snap, kubelet unix.Stat_t
	if unix.Stat(nodepath.SnapshotsDir, &snap) != nil || unix.Stat(kubeletPluginsDir, &kubelet) != nil || snap.Dev == kubelet.Dev {
		return kubeletFSHighPct, kubeletFSLowPct
	}
	return otherFSHighPct, otherFSLowPct
}
