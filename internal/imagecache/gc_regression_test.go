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

// Regression tests for defects found reviewing the Phase 2 GC POC. Each was
// written as a failing probe against the original implementation and is kept
// here, passing, to pin the fix:
//
//   - orphan layers (no surviving record references them) are reclaimed by
//     the orphan sweep, both in the interrupted-pull shape and the
//     digestless-bundle-spec shape;
//   - a pull neither shortens nor removes a longer-lived preload pin;
//   - size backfill survives directories the image made unreadable;
//   - dry-run mutates nothing, including pin files.

import (
	"archive/tar"
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// PROBE 1: a layer left in the pool with no referencing image record is
// invisible to eviction — nothing ever enumerates the layer pool looking for
// unreferenced dirs, so it can never be reclaimed. This is the state a pull
// that dies partway leaves behind (layers land individually, the record is
// written last), and the state a crash between record-removal and
// layer-retire leaves behind.
func TestOrphanLayerIsReclaimed(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/orphan:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("o", 4096)},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)

	// Simulate the interrupted-pull / crashed-eviction state: layers on disk,
	// no image record referencing them.
	if err := os.Remove(store.recordPath(img.Digest)); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	// Free-everything pass, twice (in case one pass needs to observe the
	// other's work).
	for i := 0; i < 2; i++ {
		if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
			t.Fatalf("EvictUnused: %v", err)
		}
	}

	if got := layerDirsOnDisk(t, store); len(got) != 0 {
		t.Errorf("orphan layers survive a free-everything pass: %v", got)
	}
	size, err := store.CacheSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Errorf("CacheSize() = %d after freeing everything; those bytes are unreclaimable "+
			"but still counted against --image-cache-max-bytes", size)
	}
}

// PROBE 2: EnsureImage writes a pull pin unconditionally and deletes it
// unconditionally on success, so any pull of an image clobbers a longer-lived
// pin on the same digest. Phase 3's PreloadImage writes exactly such a pin.
func TestPullPinPreservesPreloadPin(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/preloaded:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: "hi"},
	}))

	store := newTestStore(t)
	// Resolve the digest with a throwaway store so the real store still takes
	// the cache-miss path below.
	digest := mustEnsure(t, newTestStore(t), ref).Digest

	// An operator/Phase-3 preload pin: hold this image for an hour.
	if err := store.writePin(digest, pinReasonPreload, time.Hour); err != nil {
		t.Fatalf("writePin(preload): %v", err)
	}

	// A perfectly ordinary actor start pulls the same image.
	mustEnsure(t, store, ref)

	if !store.pinnedNow(digest) {
		t.Error("a pull removed the pre-existing preload pin")
	}
	b, err := os.ReadFile(store.pinPath(digest))
	if err != nil {
		t.Fatalf("pin file gone entirely: %v", err)
	}
	if !strings.Contains(string(b), pinReasonPreload) {
		t.Errorf("preload pin was overwritten by the pull pin: %s", b)
	}
}

// PROBE 3: layerSize's backfill walk aborts on the first unreadable
// directory. Images legitimately ship search-only (0o111) or 0o000
// directories, and unpackLayer preserves those modes, so a Phase-1 layer
// containing one can never be sized — it is credited 0 bytes forever, which
// makes GC永 undercount and log "could not reach target".
func TestLayerSizeBackfillSkipsUnreadableDirs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root with CAP_DAC_READ_SEARCH can list mode-0111 dirs")
	}
	store := newTestStore(t)

	dir := filepath.Join(store.layersDir(), strings.Repeat("11", 32))
	fsDir := filepath.Join(dir, layerFSDirName)
	readable := filepath.Join(fsDir, "usr")
	inner := filepath.Join(fsDir, "opt", "secret")
	for _, d := range []string{readable, inner} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(readable, "blob"), make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "blob"), make([]byte, 8192), 0o644); err != nil {
		t.Fatal(err)
	}
	// A layer that ships a search-only dir, as some distro images do.
	if err := os.Chmod(inner, 0o111); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(inner, 0o700) })

	// The contract is "never abort, count what is countable". Content under
	// a dir the image made unlistable cannot be enumerated by a process
	// without CAP_DAC_READ_SEARCH (atelet drops it), and the alternative —
	// chmod'ing the shared pool — would change the metadata overlayfs shows
	// running actors. So that content is under-counted, deliberately: an
	// under-count is consistent with the optimistic accounting elsewhere,
	// whereas the original abort returned 0 bytes *and* an error, which made
	// CacheSize skip the layer entirely and eviction credit it nothing.
	got, err := store.layerSize(dir)
	if err != nil {
		t.Errorf("layerSize aborted on a layer with an unreadable subdir: %v", err)
	}
	if got < 4096 {
		t.Errorf("layerSize = %d, want >= 4096: readable content was not counted", got)
	}

	// The size file must still be written, so the walk happens once ever.
	if _, err := os.Stat(filepath.Join(dir, layerSizeFileName)); err != nil {
		t.Errorf("backfill did not persist a size file: %v", err)
	}
	// And CacheSize must include the layer rather than skipping it.
	total, err := store.CacheSize()
	if err != nil {
		t.Fatalf("CacheSize: %v", err)
	}
	if total < 4096 {
		t.Errorf("CacheSize = %d, want >= 4096: layer omitted from the pool total", total)
	}
}

// PROBE 4: the layer-rooted-but-record-evicted path (a bundle written by a
// pre-Phase-2 atelet, i.e. every actor running across the upgrade) is a
// routine, non-crash way to manufacture the PROBE 1 orphan state: the pass
// deletes the only record referencing the layer and keeps the layer because
// LayerHexes vetoes it. Once that bundle goes away the layer is referenced by
// nothing and, per PROBE 1, is unreclaimable for the life of the node.
func TestDigestlessSpecLayersReclaimedAfterBundleGone(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/upgrade:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("u", 4096)},
	}))

	actorsDir := t.TempDir()
	store := newTestStore(t, WithActorsDir(actorsDir))
	img := mustEnsure(t, store, ref)

	// A bundle written before ImageDigest existed: layers only.
	bundle := filepath.Join(actorsDir, "actor-old", "bundles", "main")
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteSpec(bundle, &OverlaySpec{Layers: img.LayerDirs}); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	// Pass 1, while the actor runs: record goes, layer is held by LayerHexes.
	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(img.LayerDirs[0]); err != nil {
		t.Fatalf("layer of a running (digestless) actor was evicted: %v", err)
	}

	// The actor finishes; atelet removes the bundle.
	if err := os.RemoveAll(filepath.Join(actorsDir, "actor-old")); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if got := layerDirsOnDisk(t, store); len(got) != 0 {
		t.Errorf("layer left unreferenced by pass 1 is unreclaimable: %v", got)
	}
}

// PROBE 5: --image-cache-gc-dry-run is documented as "compute and log
// eviction decisions without deleting anything", but the pass unconditionally
// runs sweepExpiredPins, which deletes pin files.
func TestDryRunLeavesPinFilesAlone(t *testing.T) {
	store := newTestStore(t)
	digest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ef", 32)}
	if err := store.writePin(digest, pinReasonPull, -time.Second); err != nil {
		t.Fatal(err)
	}

	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, true); err != nil {
		t.Fatalf("EvictUnused(dry): %v", err)
	}
	if _, err := os.Stat(store.pinPath(digest)); os.IsNotExist(err) {
		t.Error("dry-run pass deleted an on-disk pin file")
	}
}
