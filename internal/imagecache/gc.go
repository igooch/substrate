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

package imagecache

// Garbage collection for the layer pool: Phase 2 of
// https://github.com/agent-substrate/substrate/issues/463.
//
// The pool is a two-level DAG — image records reference layers by diffID —
// so liveness is plain refcounting recomputed from disk on every pass, in
// the kubelet's stateless style; there is nothing to maintain between
// passes and nothing to rebuild after a restart. An image (and then any
// layer left unreferenced) is evictable unless vetoed by, in order:
//
//  1. the root set — bundle overlay specs under the actors dir (actors
//     placed on this node; the same authority that hands out mounts, since
//     atelet cannot see the ateoms' mount namespaces) and unexpired pins
//     (in-flight pulls now, Phase 3 preloads later);
//  2. min-age — records/layers younger than minAge are never touched,
//     covering the pull→spec-write→mount window;
//  3. per-victim re-checks immediately before acting (the kubelet's
//     lastUsed-vs-freeTime double check, adapted).
//
// Deletion is two-phase (containerd's shape): the only steps that contend
// with the pull path are one os.Remove of a record and one rename(2) of a
// layer dir to a ".rm-*" name inside the layer's singleflight; the slow
// RemoveAll of multi-GB trees happens afterwards, on dirs nothing can
// reach by diffid. A crash in between leaves a ".rm-*" dir for the startup
// sweep. Nothing here needs privileges: eviction is chmod/rename/unlink,
// which plain root can do even on read-only trees and whiteout devices.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sync/errgroup"
)

// countingReader counts bytes read through it; used to record a layer's
// size from the uncompressed tar stream at unpack time.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// --- pins ---

const (
	pinReasonPull    = "pull"
	pinReasonPreload = "preload" // written by Phase 3's PreloadImage
)

// pinRecord is the persisted form of an expiring pin, stored under
// pins/<algorithm>/<hex>.json. Expiry is lazy: nothing fires when a pin
// lapses; readers just skip (and eviction deletes) pins past their
// timestamp.
type pinRecord struct {
	Version int       `json:"version"`
	Reason  string    `json:"reason"`
	Expires time.Time `json:"expires"`
}

func (s *Store) pinPath(digest v1.Hash) string {
	return filepath.Join(s.root, "pins", digest.Algorithm, digest.Hex+".json")
}

// writePin creates or refreshes the expiring pin for digest.
func (s *Store) writePin(digest v1.Hash, reason string, ttl time.Duration) error {
	b, err := json.Marshal(pinRecord{Version: 1, Reason: reason, Expires: time.Now().Add(ttl)})
	if err != nil {
		return fmt.Errorf("while encoding pin: %w", err)
	}
	path := s.pinPath(digest)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("while creating pin temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("while writing pin: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing pin: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("while moving pin into place: %w", err)
	}
	slog.Info("Image cache pin written",
		slog.String("digest", digest.String()),
		slog.String("reason", reason),
		slog.Duration("ttl", ttl))
	return nil
}

func (s *Store) removePin(digest v1.Hash) {
	if err := os.Remove(s.pinPath(digest)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("Failed to remove pin", slog.String("digest", digest.String()), slog.Any("err", err))
		}
		return
	}
	slog.Info("Image cache pin removed", slog.String("digest", digest.String()))
}

// pinnedNow reports whether digest has an unexpired pin on disk right now.
// Used both for the candidate list and re-checked per victim immediately
// before deletion, so a pin written after the list was built still vetoes.
func (s *Store) pinnedNow(digest v1.Hash) bool {
	b, err := os.ReadFile(s.pinPath(digest))
	if err != nil {
		return false
	}
	var p pinRecord
	if err := json.Unmarshal(b, &p); err != nil {
		// An unreadable pin is treated as live: fail toward retention.
		return true
	}
	return time.Now().Before(p.Expires)
}

// sweepExpiredPins deletes pin files past their expiry (and orphaned pin
// temp files). Called at startup and at the end of every eviction pass.
func (s *Store) sweepExpiredPins() error {
	entries, err := os.ReadDir(s.pinsDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("while listing pins: %w", err)
	}
	for _, e := range entries {
		p := filepath.Join(s.pinsDir(), e.Name())
		if strings.HasPrefix(e.Name(), ".") {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while sweeping orphaned pin temp file %q: %w", p, err)
			}
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var pin pinRecord
		if err := json.Unmarshal(b, &pin); err != nil {
			continue // unreadable: retained (see pinnedNow)
		}
		if time.Now().After(pin.Expires) {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while sweeping expired pin %q: %w", p, err)
			}
			slog.Info("Image cache removed expired pin",
				slog.String("pin", e.Name()),
				slog.String("reason", pin.Reason),
				slog.Time("expired", pin.Expires))
		}
	}
	return nil
}

// --- sizes and last-use ---

// touchRecord refreshes the manifest record's mtime, the persisted
// last-use timestamp eviction sorts by. Best-effort: a failed touch only
// costs LRU accuracy, never correctness.
func (s *Store) touchRecord(digest v1.Hash) {
	now := time.Now()
	if err := os.Chtimes(s.recordPath(digest), now, now); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("Failed to touch image record", slog.String("digest", digest.String()), slog.Any("err", err))
	}
}

// layerSize returns the layer's recorded byte count, backfilling the size
// file (one tree walk, once ever) for layers unpacked before sizes were
// recorded.
func (s *Store) layerSize(layerDir string) (int64, error) {
	b, err := os.ReadFile(filepath.Join(layerDir, layerSizeFileName))
	if err == nil {
		n, perr := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if perr == nil {
			return n, nil
		}
		// Fall through to backfill on a corrupt size file.
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("while reading layer size: %w", err)
	}

	var total int64
	err = filepath.WalkDir(filepath.Join(layerDir, layerFSDirName), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("while backfilling size of %q: %w", layerDir, err)
	}
	// Best-effort: the write failing just means we backfill again next time.
	// Preserve the dir mtime (our eviction age signal) across the write.
	if fi, statErr := os.Stat(layerDir); statErr == nil {
		if err := os.WriteFile(filepath.Join(layerDir, layerSizeFileName), []byte(strconv.FormatInt(total, 10)+"\n"), 0o600); err == nil {
			_ = os.Chtimes(layerDir, fi.ModTime(), fi.ModTime())
		}
	}
	return total, nil
}

// CacheSize returns the sum of recorded sizes of every layer in the pool.
// It is the cache's own accounting (for --image-cache-max-bytes), distinct
// from the volume usage the watermarks run on.
func (s *Store) CacheSize() (int64, error) {
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		return 0, fmt.Errorf("while listing layer pool: %w", err)
	}
	var total int64
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.IsDir() {
			continue
		}
		n, err := s.layerSize(filepath.Join(s.layersDir(), e.Name()))
		if err != nil {
			slog.Warn("Failed to size layer", slog.String("layer", e.Name()), slog.Any("err", err))
			continue
		}
		total += n
	}
	return total, nil
}

// --- root set ---

// RootSet is the set of images and layers that eviction must not touch,
// recomputed from disk at the start of every pass (and by Phase 3's
// NodeInventory reporting).
type RootSet struct {
	// ImageDigests are rooted image digest strings ("sha256:<hex>").
	ImageDigests map[string]bool
	// LayerHexes are rooted layer diffID hexes (the layer dir base names),
	// covering bundle specs written before ImageDigest existed and
	// belt-and-suspenders for those written after.
	LayerHexes map[string]bool
}

// InUse scans the actors directory for bundle overlay specs and returns the
// images and layers referenced by actors currently placed on this node.
// Bundles exist exactly while an actor is running or mid-transition here —
// Run/Restore write the spec before any ateom is asked to mount, and a
// successful Checkpoint deletes the bundle after unmount — so this scan is
// the "actively mounted" protection of #463, derived from the same
// authority that hands out mounts. Unreadable specs root nothing (logged);
// leftover bundles from crashed actors conservatively over-pin until the
// next Run/Restore/Checkpoint wipes them.
func (s *Store) InUse() RootSet {
	rs := RootSet{ImageDigests: map[string]bool{}, LayerHexes: map[string]bool{}}
	if s.actorsDir == "" {
		return rs
	}
	actorEntries, err := os.ReadDir(s.actorsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("Failed to list actors dir for image-cache root set", slog.String("dir", s.actorsDir), slog.Any("err", err))
		}
		return rs
	}
	for _, actor := range actorEntries {
		if !actor.IsDir() {
			continue
		}
		bundlesDir := filepath.Join(s.actorsDir, actor.Name(), "bundles")
		bundles, err := os.ReadDir(bundlesDir)
		if err != nil {
			continue // no bundles: actor not placed / already torn down
		}
		for _, bundle := range bundles {
			if !bundle.IsDir() {
				continue
			}
			spec, err := ReadSpec(filepath.Join(bundlesDir, bundle.Name()))
			if err != nil {
				slog.Warn("Unreadable bundle overlay spec during image-cache root-set scan",
					slog.String("bundle", filepath.Join(bundlesDir, bundle.Name())), slog.Any("err", err))
				continue
			}
			if spec == nil {
				continue
			}
			if spec.ImageDigest != "" {
				rs.ImageDigests[spec.ImageDigest] = true
				slog.Info("Image cache root-set: bundle roots image",
					slog.String("bundle", filepath.Join(actor.Name(), "bundles", bundle.Name())),
					slog.String("digest", spec.ImageDigest),
					slog.Int("layers", len(spec.Layers)))
			} else if len(spec.Layers) > 0 {
				slog.Info("Image cache root-set: digestless bundle roots layers only",
					slog.String("bundle", filepath.Join(actor.Name(), "bundles", bundle.Name())),
					slog.Int("layers", len(spec.Layers)))
			}
			for _, layerDir := range spec.Layers {
				rs.LayerHexes[filepath.Base(layerDir)] = true
			}
		}
	}
	return rs
}

// --- eviction ---

// EvictStats reports what an eviction pass did (or, dry-run, would do).
type EvictStats struct {
	// FreedBytes is the sum of recorded sizes of retired layers. Optimistic:
	// tar-stream sizes, credited at retire time; the caller's next statfs
	// self-corrects.
	FreedBytes int64
	// EvictedImages / EvictedLayers count deleted records and retired layer
	// dirs.
	EvictedImages, EvictedLayers int
	// Candidates is the number of LRU-ordered eviction candidates after all
	// listing-stage vetoes.
	Candidates int
	// RootedImages counts image records excluded because a bundle overlay
	// spec roots them (the "actively placed" protection).
	RootedImages int
	// SkippedRooted / SkippedFresh / SkippedPinned count vetoes at listing
	// time and during the pass (a veto can fire in either place; the
	// double-check re-runs them against current disk state).
	SkippedRooted, SkippedFresh, SkippedPinned int
}

type evictionCandidate struct {
	digest  v1.Hash
	modTime time.Time
	diffIDs []string // unique, in record order
}

// EvictUnused deletes least-recently-used unprotected images (and any
// layers left unreferenced) until ~targetBytes of recorded layer size has
// been freed or candidates run out. Passing math.MaxInt64 means "free
// everything eligible" (the urgent path / preload admission check). With
// dryRun it only reports what would be freed.
//
// Failed deletions are skipped, not fatal: the error return aggregates
// them, but the pass continues to the next candidate (each retries next
// pass). Concurrent passes are serialized.
func (s *Store) EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (EvictStats, error) {
	var stats EvictStats
	if targetBytes <= 0 {
		return stats, nil
	}
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	roots := s.InUse()
	cutoff := time.Now().Add(-s.minAge)

	candidates, refcount, listErr := s.listEviction(roots, cutoff, &stats)
	// listErr is deferred, not fatal: evict what was listed, report at the end.
	stats.Candidates = len(candidates)

	slog.InfoContext(ctx, "Image cache eviction pass",
		slog.Int64("target_bytes", targetBytes),
		slog.Bool("dry_run", dryRun),
		slog.Int("rooted_images", stats.RootedImages),
		slog.Int("rooted_layers", len(roots.LayerHexes)),
		slog.Int("candidates", len(candidates)),
		slog.Duration("min_age", s.minAge))

	var errs []error
	if listErr != nil {
		errs = append(errs, listErr)
	}
	var retired []string // renamed-aside dirs awaiting RemoveAll

	for _, cand := range candidates {
		if stats.FreedBytes >= targetBytes {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		// Double-check every veto against *current* disk state: a pull, pin,
		// or bundle placement may have happened since the list was built.
		// Held exclusive against the cache-hit path (see hitMu) so a hit's
		// last-use touch and this re-check cannot interleave.
		skip, err := func() (skip string, err error) {
			s.hitMu.Lock()
			defer s.hitMu.Unlock()
			if s.pinnedNow(cand.digest) {
				return "pinned", nil
			}
			fi, err := os.Stat(s.recordPath(cand.digest))
			if errors.Is(err, os.ErrNotExist) {
				return "gone", nil // already gone (e.g. its multi-arch twin's pass)
			} else if err != nil {
				return "", err
			}
			if fi.ModTime().After(cutoff) {
				return "fresh", nil // touched since listing: in use moments ago
			}
			if !dryRun {
				if err := os.Remove(s.recordPath(cand.digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
					return "", fmt.Errorf("while deleting image record %s: %w", cand.digest, err)
				}
			}
			return "", nil
		}()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		switch skip {
		case "pinned", "fresh":
			slog.InfoContext(ctx, "Image cache eviction skipped image",
				slog.String("digest", cand.digest.String()),
				slog.String("reason", skip),
				slog.Time("last_used", cand.modTime))
			if skip == "pinned" {
				stats.SkippedPinned++
			} else {
				stats.SkippedFresh++
			}
			continue
		case "gone":
			continue
		}
		stats.EvictedImages++
		slog.InfoContext(ctx, "Image cache evicting image record",
			slog.String("digest", cand.digest.String()),
			slog.Time("last_used", cand.modTime),
			slog.Int("layers", len(cand.diffIDs)),
			slog.Bool("dry_run", dryRun))

		for _, hex := range cand.diffIDs {
			refcount[hex]--
			if refcount[hex] > 0 {
				slog.InfoContext(ctx, "Image cache keeping layer: still referenced",
					slog.String("diffid", hex), slog.Int("refcount", refcount[hex]))
				continue
			}
			if roots.LayerHexes[hex] {
				slog.InfoContext(ctx, "Image cache keeping layer: rooted by a bundle spec",
					slog.String("diffid", hex))
				stats.SkippedRooted++
				continue
			}
			size, retiredPath, evicted, err := s.retireLayer(hex, cutoff, dryRun)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !evicted {
				continue // already gone, or vetoed inside the singleflight re-check
			}
			slog.InfoContext(ctx, "Image cache retiring layer",
				slog.String("diffid", hex),
				slog.Int64("size_bytes", size),
				slog.Bool("dry_run", dryRun))
			if retiredPath != "" {
				retired = append(retired, retiredPath)
			}
			stats.FreedBytes += size
			stats.EvictedLayers++
		}
	}

	// The slow half, outside every lock: the retired dirs are unreachable by
	// diffid, so this contends with nothing. A crash before completion
	// leaves ".rm-*" dirs for the startup sweep.
	if len(retired) > 0 {
		tRemove := time.Now()
		g := new(errgroup.Group)
		g.SetLimit(4)
		for _, dir := range retired {
			g.Go(func() error {
				if err := RemoveAllWritable(dir); err != nil {
					return fmt.Errorf("while removing retired layer %q: %w", dir, err)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			errs = append(errs, err)
		}
		slog.InfoContext(ctx, "Image cache removed retired layer dirs",
			slog.Int("count", len(retired)),
			slog.Duration("took", time.Since(tRemove)))
	}

	if err := s.sweepExpiredPins(); err != nil {
		errs = append(errs, err)
	}
	return stats, errors.Join(errs...)
}

// listEviction builds the LRU-ordered candidate list and the layer
// refcounts over ALL image records (rooted and fresh ones included — their
// references are what keep shared layers alive), counting listing-stage
// vetoes into stats.
func (s *Store) listEviction(roots RootSet, cutoff time.Time, stats *EvictStats) ([]evictionCandidate, map[string]int, error) {
	entries, err := os.ReadDir(s.manifestsDir())
	if err != nil {
		return nil, nil, fmt.Errorf("while listing manifest records: %w", err)
	}

	refcount := map[string]int{}
	var candidates []evictionCandidate
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		hex := strings.TrimSuffix(name, ".json")
		digest := v1.Hash{Algorithm: "sha256", Hex: hex}

		b, err := os.ReadFile(filepath.Join(s.manifestsDir(), name))
		if err != nil {
			errs = append(errs, fmt.Errorf("while reading image record %s: %w", digest, err))
			continue
		}
		var rec imageRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			// An undecodable record can't produce refcounts; deleting it is
			// safe (layers it referenced become orphans, collected next pass)
			// but do so only via the normal candidate path so vetoes apply.
			errs = append(errs, fmt.Errorf("while decoding image record %s: %w", digest, err))
		}

		// Dedupe diffIDs per record (images may list a layer twice) so the
		// decrement in EvictUnused stays symmetric with this count.
		seen := map[string]bool{}
		var unique []string
		for _, d := range rec.DiffIDs {
			diffID, err := v1.NewHash(d)
			if err != nil {
				continue
			}
			if !seen[diffID.Hex] {
				seen[diffID.Hex] = true
				unique = append(unique, diffID.Hex)
				refcount[diffID.Hex]++
			}
		}

		fi, err := e.Info()
		if err != nil {
			continue
		}
		if roots.ImageDigests[digest.String()] {
			stats.RootedImages++
			continue
		}
		if fi.ModTime().After(cutoff) {
			stats.SkippedFresh++
			continue
		}
		if s.pinnedNow(digest) {
			stats.SkippedPinned++
			continue
		}
		candidates = append(candidates, evictionCandidate{digest: digest, modTime: fi.ModTime(), diffIDs: unique})
	}

	// LRU by last use, ties broken by name for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].digest.Hex < candidates[j].digest.Hex
		}
		return candidates[i].modTime.Before(candidates[j].modTime)
	})
	return candidates, refcount, errors.Join(errs...)
}

// Logged vetoes inside retireLayer's singleflight use this message so an
// e2e can grep for the exact race being exercised.
const logMsgLayerRetireVetoed = "Image cache layer retirement vetoed: recently used"

// retireLayer renames the layer dir aside for asynchronous removal,
// serialized against the pull path via the layer singleflight: ensureLayer
// both stats and mtime-touches the dir inside the same flight, so either
// the retire happens first (and a concurrent pull re-unpacks) or the touch
// happens first (and the re-check here vetoes). Returns the layer's
// recorded size, the renamed-aside path ("" on dry-run), and whether the
// layer was (or, dry-run, would be) evicted.
func (s *Store) retireLayer(hex string, cutoff time.Time, dryRun bool) (int64, string, bool, error) {
	dir := filepath.Join(s.layersDir(), hex)
	size, err := s.layerSize(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", false, nil // already gone
	} else if err != nil {
		size = 0 // unknown size: still evict, credit nothing
	}
	if dryRun {
		fi, err := os.Stat(dir)
		if err != nil || fi.ModTime().After(cutoff) {
			return 0, "", false, nil
		}
		return size, "", true, nil
	}

	var retired string
	_, err, _ = s.layerSF.Do(v1.Hash{Algorithm: "sha256", Hex: hex}.String(), func() (any, error) {
		fi, err := os.Stat(dir)
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		// In-flight re-check: a concurrent ensureLayer touched the dir if it
		// reused this layer since the pass began.
		if fi.ModTime().After(cutoff) {
			slog.Info(logMsgLayerRetireVetoed,
				slog.String("diffid", hex), slog.Time("last_used", fi.ModTime()))
			return nil, nil
		}
		dst := filepath.Join(s.layersDir(), fmt.Sprintf("%s%s-%d", retiredPrefix, hex[:12], time.Now().UnixNano()))
		if err := os.Rename(dir, dst); err != nil {
			return nil, fmt.Errorf("while retiring layer %s: %w", hex, err)
		}
		retired = dst
		return nil, nil
	})
	if err != nil {
		return 0, "", false, err
	}
	return size, retired, retired != "", nil
}
