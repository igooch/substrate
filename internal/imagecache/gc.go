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
	"sync"
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

// --- in-flight pull tracking ---

// holdInFlight marks every diffID as being produced by a live pull and
// returns the release function. Eviction refuses to retire a held layer.
// See Store.inFlightLayers for why this cannot be left to min-age.
func (s *Store) holdInFlight(diffIDs []v1.Hash) func() {
	held := make([]string, 0, len(diffIDs))
	s.inFlightMu.Lock()
	if s.inFlightLayers == nil {
		s.inFlightLayers = make(map[string]int, len(diffIDs))
	}
	for _, d := range diffIDs {
		s.inFlightLayers[d.Hex]++
		held = append(held, d.Hex)
	}
	s.inFlightMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.inFlightMu.Lock()
			defer s.inFlightMu.Unlock()
			for _, hex := range held {
				if s.inFlightLayers[hex] <= 1 {
					delete(s.inFlightLayers, hex)
					continue
				}
				s.inFlightLayers[hex]--
			}
		})
	}
}

// inFlight reports whether a live pull is producing this layer.
func (s *Store) inFlight(hex string) bool {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	return s.inFlightLayers[hex] > 0
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

// --- pins ---

const (
	pinReasonPull    = "pull"
	pinReasonPreload = "preload" // written by Phase 3's PreloadImage
)

// pinRecord is the persisted form of a digest's pins, stored under
// pins/<algorithm>/<hex>.json. Expiry is lazy: nothing fires when a pin
// lapses; readers skip and eviction deletes pins past their timestamp.
//
// Holders is keyed by reason, so independent holders of the same digest do
// not clobber each other. That matters because a pull's fixed 15m TTL can
// be *longer* than an operator's preload TTL: with a single expiry+reason
// the pull would overwrite the preload holder and then delete it outright
// on completion, silently un-pinning an image the control plane asked to
// keep. Phase 3 depends on this separation.
type pinRecord struct {
	Version int                  `json:"version"`
	Holders map[string]time.Time `json:"holders"`

	// Legacy single-holder fields, read for compatibility with pins written
	// by earlier builds and never written. Normalized into Holders on read.
	Reason  string    `json:"reason,omitempty"`
	Expires time.Time `json:"expires,omitempty"`
}

// live returns the holders that have not expired.
func (p *pinRecord) live(now time.Time) map[string]time.Time {
	out := make(map[string]time.Time, len(p.Holders))
	for reason, exp := range p.Holders {
		if now.Before(exp) {
			out[reason] = exp
		}
	}
	return out
}

func (s *Store) pinPath(digest v1.Hash) string {
	return filepath.Join(s.root, "pins", digest.Algorithm, digest.Hex+".json")
}

// readPin returns the digest's pin record, or (nil, nil) if there is none.
// Any other error is returned; callers must treat an error as "pinned"
// (fail toward retention) rather than assume the digest is unprotected.
func (s *Store) readPin(digest v1.Hash) (*pinRecord, error) {
	b, err := os.ReadFile(s.pinPath(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var p pinRecord
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("while decoding pin for %s: %w", digest, err)
	}
	if p.Holders == nil {
		p.Holders = map[string]time.Time{}
	}
	if p.Reason != "" && !p.Expires.IsZero() {
		if cur, ok := p.Holders[p.Reason]; !ok || p.Expires.After(cur) {
			p.Holders[p.Reason] = p.Expires
		}
	}
	return &p, nil
}

// writePin adds or extends this holder's pin on digest, leaving other
// holders untouched. An existing holder's expiry is only ever pushed out,
// never pulled in.
func (s *Store) writePin(digest v1.Hash, reason string, ttl time.Duration) error {
	expires := time.Now().Add(ttl)
	holders := map[string]time.Time{}
	if cur, err := s.readPin(digest); err == nil && cur != nil {
		holders = cur.live(time.Now())
	}
	if existing, ok := holders[reason]; !ok || expires.After(existing) {
		holders[reason] = expires
	}
	if err := s.writePinRecord(digest, holders); err != nil {
		return err
	}
	slog.Info("Image cache pin written",
		slog.String("digest", digest.String()),
		slog.String("reason", reason),
		slog.Duration("ttl", ttl))
	return nil
}

// writePinRecord atomically persists holders, removing the file when no
// holder remains.
func (s *Store) writePinRecord(digest v1.Hash, holders map[string]time.Time) error {
	path := s.pinPath(digest)
	if len(holders) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("while removing empty pin: %w", err)
		}
		return nil
	}
	b, err := json.Marshal(pinRecord{Version: 1, Holders: holders})
	if err != nil {
		return fmt.Errorf("while encoding pin: %w", err)
	}
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
	return nil
}

// removePin releases only this holder's pin; other holders survive.
func (s *Store) removePin(digest v1.Hash, reason string) {
	cur, err := s.readPin(digest)
	if err != nil || cur == nil {
		return
	}
	holders := cur.live(time.Now())
	if _, held := holders[reason]; !held {
		return
	}
	delete(holders, reason)
	if err := s.writePinRecord(digest, holders); err != nil {
		slog.Warn("Failed to release pin", slog.String("digest", digest.String()),
			slog.String("reason", reason), slog.Any("err", err))
		return
	}
	slog.Info("Image cache pin removed", slog.String("digest", digest.String()), slog.String("reason", reason))
}

// pinnedNow reports whether digest has any unexpired holder right now.
// Used both for the candidate list and re-checked per victim immediately
// before deletion, so a pin written after the list was built still vetoes.
// Read errors count as pinned: an unreadable pin must not be read as
// permission to delete.
func (s *Store) pinnedNow(digest v1.Hash) bool {
	cur, err := s.readPin(digest)
	if err != nil {
		return true
	}
	if cur == nil {
		return false
	}
	return len(cur.live(time.Now())) > 0
}

// sweepExpiredPins drops expired holders (and pin files left with none),
// plus orphaned pin temp files. Called at startup and at the end of every
// eviction pass.
func (s *Store) sweepExpiredPins() error {
	entries, err := os.ReadDir(s.pinsDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("while listing pins: %w", err)
	}
	now := time.Now()
	for _, e := range entries {
		p := filepath.Join(s.pinsDir(), e.Name())
		if strings.HasPrefix(e.Name(), ".") {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while sweeping orphaned pin temp file %q: %w", p, err)
			}
			continue
		}
		hex, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok {
			continue
		}
		digest := v1.Hash{Algorithm: "sha256", Hex: hex}
		cur, err := s.readPin(digest)
		if err != nil || cur == nil {
			continue // unreadable: retained (see pinnedNow)
		}
		live := cur.live(now)
		if len(live) == len(cur.Holders) {
			continue
		}
		if err := s.writePinRecord(digest, live); err != nil {
			return fmt.Errorf("while pruning expired pin %q: %w", p, err)
		}
		slog.Info("Image cache pruned expired pin holders",
			slog.String("digest", digest.String()),
			slog.Int("remaining", len(live)))
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
	// SkippedRooted / SkippedFresh / SkippedPinned count vetoes at listing
	// time and during the pass (a veto can fire in either place; the
	// double-check re-runs them against current disk state).
	SkippedRooted, SkippedFresh, SkippedPinned int
	// OrphanLayers counts layer dirs reclaimed by the orphan sweep: layers
	// no surviving image record references, which the record-driven pass
	// can never reach. Their bytes are included in FreedBytes.
	OrphanLayers int
}

type evictionCandidate struct {
	digest  v1.Hash
	modTime time.Time
	diffIDs []string // unique, in record order
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
// deleted or renamed and no pin files are swept; the stats report what the
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

	candidates, refcount, complete, listErr := s.listEviction(roots, cutoff, &stats)
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

	// Orphan sweep: layers no surviving record references. refcount holds
	// references from every record that existed at listing time, decremented
	// above for the records this pass deleted, so a zero (or absent) entry
	// means unreferenced right now. The rooted and min-age vetoes still
	// apply — the latter is what keeps a concurrent pull's freshly unpacked
	// layers (whose record isn't written yet) out of the sweep, reusing the
	// same singleflight + mtime interlock as retireLayer.
	// Only when the record enumeration was complete: a layer looks
	// unreferenced both when it truly is and when we failed to read the
	// record that references it. Deferring orphan collection by a pass
	// costs nothing — orphans are not urgent — whereas sweeping on partial
	// data deletes a live cache.
	if complete {
		orphans, orphanErrs := s.sweepOrphanLayers(ctx, roots, refcount, cutoff, dryRun, &stats)
		retired = append(retired, orphans...)
		errs = append(errs, orphanErrs...)
	} else {
		slog.WarnContext(ctx, "Image cache skipping orphan sweep: image records could not be fully enumerated")
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

	if !dryRun {
		if err := s.sweepExpiredPins(); err != nil {
			errs = append(errs, err)
		}
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
		size, retiredPath, evicted, err := s.retireLayer(hex, cutoff, dryRun)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !evicted {
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
	return candidates, refcount, complete, errors.Join(errs...)
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
		// A live pull is producing this layer: its record does not exist
		// yet, so neither refcounts nor pins can speak for it. Checked
		// inside the flight so it cannot go stale between here and the
		// rename, and applied to both callers — the record-driven path can
		// hit the same window when an evicted image shares a layer with an
		// image being pulled.
		if s.inFlight(hex) {
			slog.Info(logMsgLayerRetireVetoed+" (in-flight pull)", slog.String("diffid", hex))
			return nil, nil
		}
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
		short := hex
		if len(short) > 12 {
			short = short[:12]
		}
		dst := filepath.Join(s.layersDir(), fmt.Sprintf("%s%s-%d", retiredPrefix, short, time.Now().UnixNano()))
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
