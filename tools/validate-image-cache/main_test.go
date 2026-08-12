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

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/imagecache"
)

func TestRunConfigValidate(t *testing.T) {
	valid := runConfig{cacheDir: "/c", refsFile: "r.txt", evictIdle: 10 * time.Minute, minFreeGB: 150}
	cases := []struct {
		name    string
		mutate  func(*runConfig)
		wantErr string // substring of the error; "" means valid
		usage   bool   // wraps errUsage
	}{
		{"refs mode defaults", func(c *runConfig) {}, "", false},
		{"missing cache-dir", func(c *runConfig) { c.cacheDir = "" }, "exactly one", true},
		{"neither mode", func(c *runConfig) { c.refsFile = "" }, "exactly one", true},
		{"both modes", func(c *runConfig) { c.evictAll = true }, "exactly one", true},
		{"negative idle rejected everywhere", func(c *runConfig) { c.evictIdle = -time.Hour }, "must be >= 0", false},
		{"low idle fine on a validation host", func(c *runConfig) { c.evictIdle = 10 * time.Second }, "", false},
		{"low idle floored on a live node", func(c *runConfig) { c.evictIdle = 10 * time.Second; c.live = true; c.force = true }, "is below", false},
		{"live node refuses without force", func(c *runConfig) { c.live = true }, "--force", false},
		{"live node with force", func(c *runConfig) { c.live = true; c.force = true }, "", false},
		{"live flush refuses without force", func(c *runConfig) { c.refsFile = ""; c.evictAll = true; c.live = true }, "--force", false},
		{"flush with a refs-mode flag", func(c *runConfig) {
			c.refsFile = ""
			c.evictAll = true
			c.setFlags = []string{"cache-dir", "evict-all", "parallel"}
		}, "only valid with --refs-file", true},
		{"flush with its own flags", func(c *runConfig) {
			c.refsFile = ""
			c.evictAll = true
			c.setFlags = []string{"cache-dir", "evict-idle", "evict-all", "force"}
		}, "", false},
		{"future flags fail closed", func(c *runConfig) {
			c.refsFile = ""
			c.evictAll = true
			c.setFlags = []string{"some-new-flag"}
		}, "only valid with --refs-file", true},
		{"min-free-gb overflow", func(c *runConfig) { c.minFreeGB = math.MaxUint64 }, "overflows", false},
		{"min-free-gb past the int64 target", func(c *runConfig) { c.minFreeGB = 10_000_000_000 }, "overflows", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.mutate(&c)
			err := c.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate() = %v, want error containing %q", err, tc.wantErr)
			}
			if got := errors.Is(err, errUsage); got != tc.usage {
				t.Errorf("errors.Is(err, errUsage) = %v, want %v", got, tc.usage)
			}
		})
	}
}

func TestLooksLikeLiveNode(t *testing.T) {
	dir := t.TempDir()
	if !looksLikeLiveNode(dir) {
		t.Error("existing dir: want true")
	}
	if looksLikeLiveNode(filepath.Join(dir, "missing")) {
		t.Error("missing dir: want false")
	}
	if os.Geteuid() != 0 {
		// The point of the helper: an unreadable actors dir is still a
		// node, not a validation host. (Root sees through the chmod.)
		locked := filepath.Join(dir, "locked")
		if err := os.MkdirAll(filepath.Join(locked, "actors"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		if !looksLikeLiveNode(filepath.Join(locked, "actors")) {
			t.Error("EACCES actors dir: want true")
		}
	}
}

type fakeEvictor struct {
	calls     int
	gotTarget int64
	stats     imagecache.EvictStats
	err       error
}

func (f *fakeEvictor) EvictUnused(_ context.Context, target int64, _ bool) (imagecache.EvictStats, error) {
	f.calls++
	f.gotTarget = target
	return f.stats, f.err
}

func TestLowWaterCooldown(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	fake := &fakeEvictor{} // zero stats: every pass is fruitless
	l := &lowWater{
		store: fake, root: "/x", minFree: 100,
		free: func(string) uint64 { return 10 },
		now:  func() time.Time { return now },
	}

	l.evictIfLow(ctx)
	if fake.gotTarget != 90 {
		t.Errorf("target = %d, want 90 (minFree - free)", fake.gotTarget)
	}
	l.evictIfLow(ctx)
	if fake.calls != 1 {
		t.Errorf("calls after fruitless pass + immediate retry = %d, want 1 (cooldown)", fake.calls)
	}

	now = now.Add(fruitlessCooldown)
	l.evictIfLow(ctx)
	if fake.calls != 2 {
		t.Errorf("calls once the cooldown elapsed = %d, want 2", fake.calls)
	}

	// A productive pass must not arm the cooldown, even at zero bytes
	// credited (unreadable size files).
	now = now.Add(fruitlessCooldown)
	fake.stats = imagecache.EvictStats{EvictedLayers: 1}
	l.evictIfLow(ctx)
	l.evictIfLow(ctx)
	if fake.calls != 4 {
		t.Errorf("calls after productive passes = %d, want 4 (no cooldown)", fake.calls)
	}

	// Above the floor: no pass at all.
	l.free = func(string) uint64 { return 200 }
	l.evictIfLow(ctx)
	if fake.calls != 4 {
		t.Errorf("calls with free space above the floor = %d, want 4", fake.calls)
	}
}

func TestFreeGBNamesTheSentinel(t *testing.T) {
	if got := freeGB(filepath.Join(t.TempDir(), "missing")); got != "unknown" {
		t.Errorf("freeGB(missing path) = %q, want \"unknown\"", got)
	}
}
