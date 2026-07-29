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

// The image-cache GC loop: Phase 2 of
// https://github.com/agent-substrate/substrate/issues/463.
//
// A single serialized pass on a fixed period (the kubelet's shape — the
// heavy deletion work happens outside the pull path's locks, so there is
// nothing to duty-cycle). Each pass measures the cache volume with statfs
// and the pool's own recorded size, computes how many bytes to free —
// down to the low watermark when volume usage crossed the high one, and/or
// down to --image-cache-max-bytes when the pool outgrew it — and hands the
// larger target to Store.EvictUnused.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"
)

var (
	imageCacheGCPeriod = pflag.Duration("image-cache-gc-period", 5*time.Minute, "How often to run the image cache eviction pass. 0 disables eviction entirely.")
	imageCacheHighPct  = pflag.Int("image-cache-high-percent", 85, "Cache-volume usage percentage above which eviction starts.")
	imageCacheLowPct   = pflag.Int("image-cache-low-percent", 80, "Cache-volume usage percentage eviction frees down to. Must be lower than --image-cache-high-percent.")
	imageCacheMaxBytes = pflag.Int64("image-cache-max-bytes", 0, "Absolute cap on the summed size of cached layers, evicted down to independently of the volume watermarks. 0 means no cap.")
	imageCacheMinAge   = pflag.Duration("image-cache-min-age", 2*time.Minute, "Layers and image records younger than this are never evicted (protects images pulled but not yet mounted).")
	imageCachePinTTL   = pflag.Duration("image-cache-pull-pin-ttl", 15*time.Minute, "TTL of the expiring pin protecting each in-flight image pull from eviction; only matters if atelet dies mid-pull.")
	imageCacheGCDryRun = pflag.Bool("image-cache-gc-dry-run", false, "Compute and log eviction decisions without deleting anything.")
)

func validateImageCacheGCFlags() error {
	if *imageCacheHighPct < 1 || *imageCacheHighPct > 100 {
		return fmt.Errorf("--image-cache-high-percent %d out of range [1,100]", *imageCacheHighPct)
	}
	if *imageCacheLowPct < 0 || *imageCacheLowPct >= *imageCacheHighPct {
		return fmt.Errorf("--image-cache-low-percent %d must be in [0,%d)", *imageCacheLowPct, *imageCacheHighPct)
	}
	return nil
}

// imageCacheGCTarget computes the bytes an eviction pass should free.
// Watermark half (kubelet's formula): when usage is at or above the high
// watermark, free down to the low one. Cap half: when the pool's recorded
// size exceeds maxBytes, free the difference. The pass frees the larger.
func imageCacheGCTarget(capacity, available uint64, cacheSize, maxBytes int64, highPct, lowPct int) int64 {
	var target int64
	if capacity > 0 {
		usedPct := 100 - int(available*100/capacity)
		if usedPct >= highPct {
			// Free enough that available climbs back to (100-lowPct)% of
			// capacity.
			target = int64(capacity)*int64(100-lowPct)/100 - int64(available)
		}
	}
	if maxBytes > 0 && cacheSize > maxBytes {
		if over := cacheSize - maxBytes; over > target {
			target = over
		}
	}
	return target
}

// runImageCacheGC runs eviction passes on the configured period until ctx
// is done. Passes are strictly serialized: a slow pass delays the next
// tick rather than overlapping it.
func runImageCacheGC(ctx context.Context, store *imagecache.Store, cacheDir string) {
	ticker := time.NewTicker(*imageCacheGCPeriod)
	defer ticker.Stop()

	consecutiveShortfalls := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		var st unix.Statfs_t
		if err := unix.Statfs(cacheDir, &st); err != nil {
			slog.WarnContext(ctx, "Image cache GC: statfs failed", slog.String("dir", cacheDir), slog.Any("err", err))
			continue
		}
		capacity := st.Blocks * uint64(st.Bsize)
		available := st.Bavail * uint64(st.Bsize)

		cacheSize, err := store.CacheSize()
		if err != nil {
			slog.WarnContext(ctx, "Image cache GC: sizing the pool failed", slog.Any("err", err))
			continue
		}

		target := imageCacheGCTarget(capacity, available, cacheSize, *imageCacheMaxBytes, *imageCacheHighPct, *imageCacheLowPct)
		if target <= 0 {
			consecutiveShortfalls = 0
			continue
		}

		tStart := time.Now()
		stats, err := store.EvictUnused(ctx, target, *imageCacheGCDryRun)
		attrs := []any{
			slog.Int64("target_bytes", target),
			slog.Int64("freed_bytes", stats.FreedBytes),
			slog.Int("evicted_images", stats.EvictedImages),
			slog.Int("evicted_layers", stats.EvictedLayers),
			slog.Int("candidates", stats.Candidates),
			slog.Int("rooted_images", stats.RootedImages),
			slog.Int("skipped_rooted", stats.SkippedRooted),
			slog.Int("skipped_fresh", stats.SkippedFresh),
			slog.Int("skipped_pinned", stats.SkippedPinned),
			slog.Int64("cache_size_bytes", cacheSize),
			slog.Bool("dry_run", *imageCacheGCDryRun),
			slog.Duration("took", time.Since(tStart)),
		}
		if err != nil {
			// Per-item failures were already skipped inside the pass; log the
			// aggregate and let the next pass retry.
			slog.WarnContext(ctx, "Image cache GC pass finished with errors", append(attrs, slog.Any("err", err))...)
		}
		if stats.FreedBytes < target {
			// Everything eligible was evicted and the target still wasn't
			// met: the remainder is rooted, pinned, fresh, or not ours (other
			// state on the shared volume). Escalate only when persistent.
			consecutiveShortfalls++
			if consecutiveShortfalls > 1 {
				slog.ErrorContext(ctx, "Image cache GC repeatedly unable to reach target; volume pressure is from protected images or non-cache data", attrs...)
			} else {
				slog.WarnContext(ctx, "Image cache GC could not reach target", attrs...)
			}
		} else {
			consecutiveShortfalls = 0
			slog.InfoContext(ctx, "Image cache GC pass complete", attrs...)
		}
	}
}
