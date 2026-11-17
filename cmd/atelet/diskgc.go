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
	"fmt"

	"github.com/spf13/pflag"

	"go.ate.dev/substrate/cmd/atelet/internal/diskgc"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/imagecache"
	"k8s.io/utils/clock"
)

var (
	diskGCPeriod  = pflag.Duration("disk-gc-period", 0, "How often to check each filesystem atelet writes to and free space on the ones over --disk-gc-high-percent. 0 disables it. While on, the image cache GC's own watermark (--image-cache-high-percent) still runs too.")
	diskGCHighPct = pflag.Int("disk-gc-high-percent", 80, "Filesystem usage percentage above which disk GC frees space.")
	diskGCLowPct  = pflag.Int("disk-gc-low-percent", 75, "Filesystem usage percentage disk GC frees down to. Must be lower than --disk-gc-high-percent.")
	diskGCDryRun  = pflag.Bool("disk-gc-dry-run", false, "Log what disk GC would free without deleting anything.")
)

// validateDiskGCFlags rejects a negative period, which would silently disable
// the disk GC, and bad watermarks, before atelet clears any cache.
func validateDiskGCFlags() error {
	if *diskGCPeriod < 0 {
		return fmt.Errorf("--disk-gc-period %v must be >= 0", *diskGCPeriod)
	}
	if err := diskgc.CheckWatermarks(*diskGCHighPct, *diskGCLowPct); err != nil {
		return fmt.Errorf("--disk-gc-high-percent and --disk-gc-low-percent: %w", err)
	}
	return nil
}

// startDiskGC runs the disk GC when --disk-gc-period is set. snapshotCache is
// nil when that cache is disabled.
func startDiskGC(ctx context.Context, imageCache *imagecache.Store, sandboxAssetCache, snapshotCache *filecache.Store) error {
	if *diskGCPeriod <= 0 {
		return nil
	}
	// Freeing order, cheapest data first.
	stores := []diskgc.Store{
		{
			Name: "image_cache",
			Dir:  *imageCacheDir,
			Used: func(context.Context) (int64, error) { return imageCache.CacheSize() },
			Free: func(ctx context.Context, target int64, dryRun bool) (int64, error) {
				stats, err := imageCache.EvictUnused(ctx, target, dryRun)
				return stats.FreedBytes, err
			},
		},
		fileCacheStore(ateattr.FileCacheSandboxAsset, *sandboxAssetCacheDir, sandboxAssetCache),
	}
	if snapshotCache != nil {
		stores = append(stores, fileCacheStore(ateattr.FileCacheSnapshot, *snapshotCacheDir, snapshotCache))
	}
	c, err := diskgc.New(diskgc.HostFS{}, clock.RealClock{}, *diskGCHighPct, *diskGCLowPct, *diskGCDryRun, stores)
	if err != nil {
		return err
	}
	go c.Run(ctx, *diskGCPeriod)
	return nil
}

// fileCacheStore is the filecache store called name, rooted at dir.
func fileCacheStore(name, dir string, store *filecache.Store) diskgc.Store {
	return diskgc.Store{
		Name: name,
		Dir:  dir,
		Used: store.TotalBytes,
		Free: func(ctx context.Context, target int64, dryRun bool) (int64, error) {
			stats, err := store.EvictUnused(ctx, target, dryRun)
			return stats.FreedBytes, err
		},
	}
}
