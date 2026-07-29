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
//     atelet cannot see the ateoms' mount namespaces);
//  2. min-age — records/layers younger than minAge are never touched,
//     covering the pull→spec-write→mount window;
//  3. per-victim re-checks immediately before acting (the kubelet's
//     lastUsed-vs-freeTime double check, adapted).
//
// The periodic pass reaches layers ONLY through records: pull writes the
// image record before unpacking (see Store.pull), so every layer is
// referenced — and thereby protected — before it can exist, the way Go
// allocates black during GC, containerd creates the ingest record in the
// transaction that opens the writer, and ext4 journals the orphan entry
// with the O_TMPFILE allocation. Unexplained layers can therefore only be
// crash debris, reclaimed once at startup by RecoverOrphans; there is no
// online whole-pool scan (ext4's split: bounded recovery at mount, fsck
// offline). Pins return in Phase 3 for preloads, where the record their
// digest resolves through always exists.
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

// isLayerDirName reports whether name is a well-formed sha256 layer
// directory name. The orphan sweep enumerates the pool directly, so it can
// encounter anything an operator left there; only conforming names are
// treated as layers (and only they are safe to abbreviate when renaming
// aside).
func isLayerDirName(name string) bool {
	if len(name) != 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
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
	fsRoot := filepath.Join(layerDir, layerFSDirName)
	err = filepath.WalkDir(fsRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Images legitimately ship unreadable directories (0111, 0000)
			// and unpack preserves their modes, so a walk error is expected,
			// not fatal: skip that subtree and keep counting. Under-counting
			// is consistent with the optimistic accounting everywhere else.
			if p != fsRoot && d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
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
	// LayerSets holds a signature per rooted bundle's *exact* layer set.
	// A record whose layer set matches one is rooted too: that is the
	// multi-arch twin (same image recorded under the index and the
	// per-platform child digest, identical layers) and the digestless
	// pre-Phase-2 spec, whose record would otherwise be evicted while its
	// layers survive — manufacturing an orphan the moment the bundle goes.
	//
	// Deliberately an exact match, not "every layer is rooted somewhere":
	// the union of all rooted layers makes any image whose layers are a
	// subset — e.g. the base image of a running actor's image —
	// permanently unevictable, which quietly weakens --image-cache-max-bytes
	// on nodes with heavy layer sharing.
	LayerSets map[string]bool
}

// layerSetSignature canonicalizes a set of layer hexes for comparison.
func layerSetSignature(hexes []string) string {
	uniq := make([]string, 0, len(hexes))
	seen := make(map[string]bool, len(hexes))
	for _, h := range hexes {
		if !seen[h] {
			seen[h] = true
			uniq = append(uniq, h)
		}
	}
	sort.Strings(uniq)
	return strings.Join(uniq, ",")
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
	rs := RootSet{ImageDigests: map[string]bool{}, LayerHexes: map[string]bool{}, LayerSets: map[string]bool{}}
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
			// WriteSpec renames its temp file into place, so a spec read here
			// is always whole; a partial read would under-report an actor's
			// layers and fail toward deleting them.
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
			hexes := make([]string, 0, len(spec.Layers))
			for _, layerDir := range spec.Layers {
				hex := filepath.Base(layerDir)
				rs.LayerHexes[hex] = true
				hexes = append(hexes, hex)
			}
			if len(hexes) > 0 {
				rs.LayerSets[layerSetSignature(hexes)] = true
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
	// SkippedRooted / SkippedFresh count vetoes at listing time and during
	// the pass (a veto can fire in either place; the double-check re-runs
	// them against current disk state).
	SkippedRooted, SkippedFresh int
	// OrphanLayers counts layer dirs reclaimed by the STARTUP orphan scan
	// (RecoverOrphans): layers no record references, which the record-driven
	// pass can never reach. Always zero for periodic passes — a live process
	// cannot create orphans now that pull writes the record before
	// unpacking, so the scan runs only at startup (ext4's shape: bounded
	// recovery at mount, no online fsck). Bytes are included in FreedBytes.
	OrphanLayers int
}

type evictionCandidate struct {
	digest  v1.Hash
	modTime time.Time
	diffIDs []string // unique, in record order
	raw     []byte   // record file bytes, for restoration if a layer must be kept
}

// EvictUnused reclaims cache disk in two parts:
//
//   - an **orphan sweep**, which runs on every pass regardless of
//     targetBytes: layer dirs that no surviving image record references are
//     unreachable garbage (interrupted pulls leave them, since layers land
//     individually and the record is written last; so do digestless bundle
//     specs, whose layers outlive their record). Nothing else can ever
//     reclaim them — the record-driven pass below only reaches layers via a
//     record's diffID list — and they count against --image-cache-max-bytes
//     until it does.
//   - **targeted eviction** of least-recently-used unprotected images (and
//     the layers their removal leaves unreferenced) until ~targetBytes has
//     been freed or candidates run out. Passing math.MaxInt64 means "free
//     everything eligible" (the urgent path / preload admission check).
//
// targetBytes <= 0 runs the orphan sweep alone. With dryRun nothing is
// deleted or renamed; the stats report what the
// pass would have freed.
//
// Failed deletions are skipped, not fatal: the error return aggregates
// them, but the pass continues to the next candidate (each retries next
// pass). Concurrent passes are serialized.
func (s *Store) EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (EvictStats, error) {
	var stats EvictStats
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	roots := s.InUse()
	cutoff := time.Now().Add(-s.minAge)

	candidates, refcount, _, listErr := s.listEviction(roots, cutoff, &stats)
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
		if targetBytes <= 0 || stats.FreedBytes >= targetBytes {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		// Double-check every veto against *current* disk state: a pull
		// or bundle placement may have happened since the list was built.
		// Held exclusive against the cache-hit path (see hitMu) so a hit's
		// last-use touch and this re-check cannot interleave.
		skip, err := func() (skip string, err error) {
			s.hitMu.Lock()
			defer s.hitMu.Unlock()
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
		case "fresh":
			slog.InfoContext(ctx, "Image cache eviction skipped image",
				slog.String("digest", cand.digest.String()),
				slog.String("reason", skip),
				slog.Time("last_used", cand.modTime))
			stats.SkippedFresh++
			continue
		case "gone":
			continue
		}
		slog.InfoContext(ctx, "Image cache evicting image record",
			slog.String("digest", cand.digest.String()),
			slog.Time("last_used", cand.modTime),
			slog.Int("layers", len(cand.diffIDs)),
			slog.Bool("dry_run", dryRun))

		// The record is gone (in dry-run: would be). Retire the layers its
		// removal un-references — and track any that must be KEPT. A kept
		// layer at refcount 0 with no record is unreachable by the runtime
		// pass until the next restart (review 3, finding 1: the v1 per-pass
		// sweep was the accidental backstop for exactly this state, and it
		// no longer runs). If anything is kept, the record is restored below
		// and the image simply is not evicted this pass.
		kept := false
		for _, hex := range cand.diffIDs {
			refcount[hex]--
			if refcount[hex] > 0 {
				slog.InfoContext(ctx, "Image cache keeping layer: still referenced",
					slog.String("diffid", hex), slog.Int("refcount", refcount[hex]))
				continue
			}
			if roots.LayerHexes[hex] {
				// Rooted by a bundle spec (the digestless-spec case). Without
				// its record this layer would strand the moment that bundle
				// goes away; keep the record with it.
				slog.InfoContext(ctx, "Image cache keeping layer: rooted by a bundle spec",
					slog.String("diffid", hex))
				stats.SkippedRooted++
				kept = true
				continue
			}
			size, retiredPath, st, err := s.retireLayer(hex, cutoff, dryRun)
			if err != nil {
				errs = append(errs, err)
				kept = true
				continue
			}
			switch st {
			case retireGone:
				continue
			case retireVetoed:
				kept = true
				continue
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
		if kept {
			// Put the reference back: rewrite the record so every kept layer
			// stays reachable, and restore this candidate's refcounts so
			// later victims in this pass do not under-count shared layers.
			// Already-retired layers stay retired — a record with missing
			// layers re-pulls only the gaps (stale-record-harmless).
			if !dryRun {
				if err := s.restoreRecord(cand.digest, cand.raw); err != nil {
					errs = append(errs, fmt.Errorf("while restoring record %s after kept layer: %w", cand.digest, err))
				} else {
					slog.InfoContext(ctx, "Image cache restored image record: some of its layers must be kept",
						slog.String("digest", cand.digest.String()))
				}
			}
			for _, hex := range cand.diffIDs {
				refcount[hex]++
			}
			continue
		}
		stats.EvictedImages++
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

	return stats, errors.Join(errs...)
}

// RecoverOrphans reclaims layer dirs that no image record references. It
// is called once, from New, before the store serves any request — the one
// moment the scan is structurally race-free: no pull can be in flight, so
// a layer without a record is definitionally garbage, not work in
// progress. (During normal operation orphans cannot arise: pull writes the
// record before unpacking, and eviction retires layers in the same pass
// that drops their records. What this scan reaps is crash debris — a
// crash between record-delete and layer-rename — and operator damage.
// A retireLayer failure while the process lives therefore leaks until the
// next restart: rare, logged, bounded, and the accepted trade for not
// running an fsck against a live pool every pass.)
//
// Conservative by construction: if the record enumeration is not complete
// (unreadable dir, undecodable record), the scan is skipped entirely and
// logged at ERROR — refcounts from partial data make referenced layers
// look like garbage. Bundle-spec roots and min-age still veto, covering
// actors running across the restart and any near-boundary mtimes.
func (s *Store) RecoverOrphans(ctx context.Context) (EvictStats, error) {
	var stats EvictStats
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	roots := s.InUse()
	cutoff := time.Now().Add(-s.minAge)
	_, refcount, complete, listErr := s.listEviction(roots, cutoff, &stats)
	if !complete {
		slog.ErrorContext(ctx, "Image cache startup orphan scan skipped: image records could not be fully enumerated; orphaned layers (if any) will persist until the records are repaired",
			slog.Any("err", listErr))
		return stats, listErr
	}

	retired, errs := s.sweepOrphanLayers(ctx, roots, refcount, cutoff, false, &stats)
	for _, dir := range retired {
		if err := RemoveAllWritable(dir); err != nil {
			errs = append(errs, fmt.Errorf("while removing retired orphan %q: %w", dir, err))
		}
	}
	if stats.OrphanLayers > 0 {
		slog.InfoContext(ctx, "Image cache startup scan reclaimed orphan layers",
			slog.Int("count", stats.OrphanLayers),
			slog.Int64("freed_bytes", stats.FreedBytes))
	}
	return stats, errors.Join(errs...)
}

// sweepOrphanLayers retires layer dirs that no surviving image record
// references. Returns the renamed-aside paths for the caller's async
// removal batch, plus any per-layer errors (never fatal).
func (s *Store) sweepOrphanLayers(ctx context.Context, roots RootSet, refcount map[string]int, cutoff time.Time, dryRun bool, stats *EvictStats) ([]string, []error) {
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		return nil, []error{fmt.Errorf("while listing layer pool for orphan sweep: %w", err)}
	}

	var retired []string
	var errs []error
	for _, e := range entries {
		hex := e.Name()
		// Dot-prefixed dirs are in-flight unpacks (".tmp-") or already
		// retired (".rm-"); both are handled elsewhere. Anything that is
		// not a well-formed digest was not put there by the store, so the
		// sweep leaves it alone rather than deleting an operator's file (or
		// panicking on a name too short to abbreviate).
		if !e.IsDir() || strings.HasPrefix(hex, ".") || !isLayerDirName(hex) {
			continue
		}
		if refcount[hex] > 0 {
			continue // referenced by a surviving record
		}
		if roots.LayerHexes[hex] {
			// A bundle spec references it directly — the digestless-spec
			// case, where the record is gone but an actor is still using
			// these layers.
			continue
		}
		size, retiredPath, st, err := s.retireLayer(hex, cutoff, dryRun)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if st != retireRetired {
			continue // too young, or vanished under us
		}
		slog.InfoContext(ctx, "Image cache retiring orphan layer",
			slog.String("diffid", hex),
			slog.Int64("size_bytes", size),
			slog.Bool("dry_run", dryRun))
		if retiredPath != "" {
			retired = append(retired, retiredPath)
		}
		stats.FreedBytes += size
		stats.OrphanLayers++
	}
	return retired, errs
}

// listEviction builds the LRU-ordered candidate list and the layer
// refcounts over ALL image records (rooted and fresh ones included — their
// references are what keep shared layers alive), counting listing-stage
// vetoes into stats.
//
// The returned complete flag reports whether every record was read and
// decoded. Refcounts from a partial listing understate references, which
// is safe for the record-driven pass (it only ever skips work) but fatal
// for the orphan sweep, which would read "no references" as "garbage".
func (s *Store) listEviction(roots RootSet, cutoff time.Time, stats *EvictStats) (cands []evictionCandidate, refcount map[string]int, complete bool, err error) {
	refcount = map[string]int{}
	entries, err := os.ReadDir(s.manifestsDir())
	if err != nil {
		return nil, refcount, false, fmt.Errorf("while listing manifest records: %w", err)
	}
	complete = true
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
			// This record's layers get no refcounts while the record itself
			// survives, so its layers would look like orphans.
			complete = false
			errs = append(errs, fmt.Errorf("while reading image record %s: %w", digest, err))
			continue
		}
		var rec imageRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			complete = false
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
		// A record is rooted either by digest (a bundle spec naming it) or
		// because every layer it lists is rooted. The latter covers the
		// multi-arch twin — Phase 1 records an image under both the index
		// and per-platform child digest, but a bundle spec carries only the
		// requested one — and digestless (pre-Phase-2) specs. Without it the
		// twin is evicted and rewritten on every pull of a rooted image:
		// harmless but pure churn, and it inflates the eviction counters.
		if roots.ImageDigests[digest.String()] || (len(unique) > 0 && roots.LayerSets[layerSetSignature(unique)]) {
			stats.RootedImages++
			continue
		}
		if fi.ModTime().After(cutoff) {
			stats.SkippedFresh++
			continue
		}
		candidates = append(candidates, evictionCandidate{digest: digest, modTime: fi.ModTime(), diffIDs: unique, raw: b})
	}

	// LRU by last use, ties broken by name for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].digest.Hex < candidates[j].digest.Hex
		}
		return candidates[i].modTime.Before(candidates[j].modTime)
	})
	return candidates, refcount, complete, errors.Join(errs...)
}

// restoreRecord atomically rewrites a record from the bytes captured at
// listing time. Used when eviction deleted a record but then had to keep
// one of its layers: without the record the kept layer is unreachable by
// the runtime pass. The rewrite bumps the record's mtime, so min-age keeps
// it off the next pass's candidate list — the retry happens once the kept
// layer is itself old enough to go.
func (s *Store) restoreRecord(digest v1.Hash, raw []byte) error {
	path := s.recordPath(digest)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("while creating record temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("while writing record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing record: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("while moving record into place: %w", err)
	}
	return nil
}

// retireStatus reports what retireLayer did with a layer.
type retireStatus int

const (
	// retireGone: no dir under the layer's final name — already removed, or
	// mid-unpack (only the commit rename creates the final name, and the
	// pre-written record already references it). Nothing stranded.
	retireGone retireStatus = iota
	// retireVetoed: the layer stays — fresh mtime, in-flight reuse, or a
	// failed rename. If its record was already deleted, the caller must
	// restore it, or the layer is unreachable until the next startup scan.
	retireVetoed
	// retireRetired: renamed aside for asynchronous removal.
	retireRetired
)

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
func (s *Store) retireLayer(hex string, cutoff time.Time, dryRun bool) (int64, string, retireStatus, error) {
	dir := filepath.Join(s.layersDir(), hex)

	// Pre-flight, outside the singleflight: a missing dir means a
	// mid-unpack layer (its final name appears only at the commit rename;
	// the pre-written record already references it) or an already-removed
	// one — either way nothing to do, and crucially no reason to enter the
	// flight and block behind an in-progress download (review 3, finding
	// 4: the pass used to wait out a concurrent unpack here, holding
	// evictMu the whole time). A fresh mtime is a veto for the same
	// non-blocking reason; both checks are re-run inside the flight before
	// the rename, so this is a fast path, not the correctness path.
	fi, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", retireGone, nil
	} else if err != nil {
		return 0, "", retireVetoed, err
	}
	if fi.ModTime().After(cutoff) {
		slog.Info(logMsgLayerRetireVetoed, slog.String("diffid", hex), slog.Time("last_used", fi.ModTime()))
		return 0, "", retireVetoed, nil
	}

	size, err := s.layerSize(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", retireGone, nil
	} else if err != nil {
		size = 0 // unknown size: still evict, credit nothing
	}
	if dryRun {
		return size, "", retireRetired, nil
	}

	var retired string
	status := retireGone
	_, err, _ = s.layerSF.Do(v1.Hash{Algorithm: "sha256", Hex: hex}.String(), func() (any, error) {
		fi, err := os.Stat(dir)
		if errors.Is(err, os.ErrNotExist) {
			status = retireGone
			return nil, nil
		} else if err != nil {
			status = retireVetoed
			return nil, err
		}
		// Re-check inside the flight: a concurrent ensureLayer touched the
		// dir if it reused this layer since the pre-flight stat.
		if fi.ModTime().After(cutoff) {
			slog.Info(logMsgLayerRetireVetoed, slog.String("diffid", hex), slog.Time("last_used", fi.ModTime()))
			status = retireVetoed
			return nil, nil
		}
		short := hex
		if len(short) > 12 {
			short = short[:12]
		}
		dst := filepath.Join(s.layersDir(), fmt.Sprintf("%s%s-%d", retiredPrefix, short, time.Now().UnixNano()))
		if err := os.Rename(dir, dst); err != nil {
			status = retireVetoed
			return nil, fmt.Errorf("while retiring layer %s: %w", hex, err)
		}
		retired = dst
		status = retireRetired
		return nil, nil
	})
	if err != nil {
		return 0, "", status, err
	}
	return size, retired, status, nil
}
