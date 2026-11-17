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

package diskgc

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// fakeFS maps each dir to a device, and each device to its total and
// available bytes. Stores update the available bytes as they free.
type fakeFS struct {
	devices   map[string]uint64
	space     map[uint64]*[2]uint64
	failSpace bool
}

func (f fakeFS) Device(dir string) (uint64, error) {
	d, ok := f.devices[dir]
	if !ok {
		return 0, errors.New("no such dir")
	}
	return d, nil
}

func (f fakeFS) Space(dir string) (uint64, uint64, error) {
	if f.failSpace {
		return 0, 0, errors.New("statfs failed")
	}
	space := f.space[f.devices[dir]]
	return space[0], space[1], nil
}

// fakeStore holds used bytes, frees up to its reclaimable ones, and records
// each Free call. Freeing returns space to its filesystem unless the freed
// files are still held open.
type fakeStore struct {
	name        string
	used        int64
	reclaimable int64
	calls       *[]string
	space       *[2]uint64
	heldOpen    bool
}

func (s *fakeStore) Used(context.Context) (int64, error) { return s.used, nil }

func (s *fakeStore) Free(_ context.Context, target int64, dryRun bool) (int64, error) {
	*s.calls = append(*s.calls, s.name)
	freed := min(target, s.reclaimable)
	if dryRun {
		return freed, nil
	}
	s.reclaimable -= freed
	if !s.heldOpen {
		s.space[1] += uint64(freed)
	}
	return freed, nil
}

func TestPass(t *testing.T) {
	// Two filesystems of 1000 bytes. Watermarks 80 and 70, so a filesystem at
	// 900 used targets 200. The boot filesystem holds images and assets, which
	// can free, and actor data, which is only tracked.
	tests := []struct {
		name        string
		bootAvail   uint64 // available bytes on the boot filesystem
		snapAvail   uint64 // available bytes on the snapshots filesystem
		images      int64  // reclaimable bytes in the image store
		heldOpen    bool   // the image store's freed files are still open
		wantCalls   []string
		wantResults map[string]result
	}{
		{
			name: "both under the high watermark", bootAvail: 500, snapAvail: 500, images: 1000,
			wantResults: map[string]result{"/boot": {Outcome: outcomeQuiet}, "/snap": {Outcome: outcomeQuiet}},
		},
		{
			name: "only the full filesystem's stores free", bootAvail: 500, snapAvail: 100, images: 1000,
			wantCalls: []string{"snapshots"},
			wantResults: map[string]result{
				"/boot": {Outcome: outcomeQuiet},
				"/snap": {Target: 200, Freed: 200, Measured: 200, Outcome: outcomeComplete},
			},
		},
		{
			name: "frees in order, skips tracked stores, stops at the target", bootAvail: 100, snapAvail: 500, images: 150,
			wantCalls: []string{"images", "assets"},
			wantResults: map[string]result{
				"/boot": {Target: 200, Freed: 200, Measured: 200, Outcome: outcomeComplete},
				"/snap": {Outcome: outcomeQuiet},
			},
		},
		{
			name: "shortfall when the stores run out", bootAvail: 100, snapAvail: 500, images: 50,
			wantCalls: []string{"images", "assets"},
			wantResults: map[string]result{
				"/boot": {Target: 200, Freed: 150, Measured: 150, Outcome: outcomeShortfall},
				"/snap": {Outcome: outcomeQuiet},
			},
		},
		{
			name: "measured freed is lower while freed files are still open", bootAvail: 100, snapAvail: 500, images: 150, heldOpen: true,
			wantCalls: []string{"images", "assets"},
			wantResults: map[string]result{
				"/boot": {Target: 200, Freed: 200, Measured: 50, Outcome: outcomeComplete},
				"/snap": {Outcome: outcomeQuiet},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := fakeFS{
				devices: map[string]uint64{"/snap": 2, "/boot": 1, "/boot/assets": 1, "/boot/actors": 1},
				space:   map[uint64]*[2]uint64{1: {1000, tc.bootAvail}, 2: {1000, tc.snapAvail}},
			}
			var calls []string
			snapshots := &fakeStore{name: "snapshots", used: 300, reclaimable: 1000, calls: &calls, space: fs.space[2]}
			images := &fakeStore{name: "images", used: 200, reclaimable: tc.images, calls: &calls, space: fs.space[1], heldOpen: tc.heldOpen}
			assets := &fakeStore{name: "assets", used: 100, reclaimable: 100, calls: &calls, space: fs.space[1]}
			c, _ := newCoordinator(t, fs, false, []Store{
				{Name: "snapshots", Dir: "/snap", Used: snapshots.Used, Free: snapshots.Free},
				{Name: "images", Dir: "/boot", Used: images.Used, Free: images.Free},
				// Counted only, between the two freeable boot stores, to check
				// it's skipped.
				{Name: "actors", Dir: "/boot/actors", Used: func(context.Context) (int64, error) { return 400, nil }},
				{Name: "assets", Dir: "/boot/assets", Used: assets.Used, Free: assets.Free},
			})

			results := map[string]result{}
			for _, res := range c.pass(context.Background()) {
				results[res.Dir] = res
			}
			if !slices.Equal(calls, tc.wantCalls) {
				t.Errorf("Free calls = %v, want %v", calls, tc.wantCalls)
			}
			for dir, want := range tc.wantResults {
				got := results[dir]
				if got.Target != want.Target || got.Freed != want.Freed || got.Measured != want.Measured || got.Outcome != want.Outcome {
					t.Errorf("%s: got target=%d freed=%d measured=%d outcome=%s, want %d %d %d %s",
						dir, got.Target, got.Freed, got.Measured, got.Outcome, want.Target, want.Freed, want.Measured, want.Outcome)
				}
			}
			wantBoot := map[string]int64{"images": 200, "actors": 400, "assets": 100}
			if got := results["/boot"].Stores; !maps.Equal(got, wantBoot) {
				t.Errorf("boot store usage = %v, want %v", got, wantBoot)
			}
		})
	}
}

func TestLargest(t *testing.T) {
	tests := []struct {
		name   string
		used   int64
		stores map[string]int64
		want   string
	}{
		{name: "a store holds the most", used: 1000, stores: map[string]int64{"images": 300, "actors": 500}, want: "actors"},
		{name: "untracked data holds the most", used: 1000, stores: map[string]int64{"images": 100, "actors": 200}, want: "untracked"},
		{name: "no stores", used: 1000, want: "untracked"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (result{Used: tc.used, Stores: tc.stores}).largest(); got != tc.want {
				t.Errorf("Largest() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPassRecoversFromAStorePanic(t *testing.T) {
	fs := fakeFS{devices: map[string]uint64{"/boot": 1}, space: map[uint64]*[2]uint64{1: {1000, 100}}}
	c, _ := newCoordinator(t, fs, false, []Store{{
		Name: "broken",
		Dir:  "/boot",
		Used: func(context.Context) (int64, error) { return 100, nil },
		Free: func(context.Context, int64, bool) (int64, error) { panic("boom") },
	}})
	results := c.pass(context.Background())
	if len(results) != 1 || results[0].Outcome != outcomeSkipped || results[0].Err == nil {
		t.Errorf("got %+v, want one skipped result with an error", results)
	}
}

// newCoordinator returns a Coordinator with watermarks 80 and 70, on a fake
// clock that moves only when the test steps it.
func newCoordinator(t *testing.T, fs FS, dryRun bool, stores []Store) (*Coordinator, *testingclock.FakePassiveClock) {
	t.Helper()
	clk := testingclock.NewFakePassiveClock(time.Time{})
	c, err := New(fs, clk, 80, 70, dryRun, stores)
	if err != nil {
		t.Fatal(err)
	}
	return c, clk
}

// oneFS returns a fake filesystem of 1000 bytes with avail available, holding
// /boot.
func oneFS(avail uint64) (fakeFS, *[2]uint64) {
	space := &[2]uint64{1000, avail}
	return fakeFS{devices: map[string]uint64{"/boot": 1}, space: map[uint64]*[2]uint64{1: space}}, space
}

// oneStore returns a Coordinator over one full filesystem (900 of 1000 bytes
// used, so a deficit of 200) holding store.
func oneStore(t *testing.T, dryRun bool, store *fakeStore) (*Coordinator, *[2]uint64, *testingclock.FakePassiveClock) {
	t.Helper()
	fs, space := oneFS(100)
	store.space = space
	c, clk := newCoordinator(t, fs, dryRun, []Store{{Name: store.name, Dir: "/boot", Used: store.Used, Free: store.Free}})
	return c, space, clk
}

func TestPassCapsTargetAtFreeableSize(t *testing.T) {
	var calls []string
	c, _, _ := oneStore(t, false, &fakeStore{name: "images", used: 50, reclaimable: 50, calls: &calls})
	res := c.pass(context.Background())[0]
	if res.Deficit != 200 || res.Target != 50 || res.Freed != 50 || res.Outcome != outcomeShortfall {
		t.Errorf("got deficit=%d target=%d freed=%d outcome=%s, want 200 50 50 shortfall", res.Deficit, res.Target, res.Freed, res.Outcome)
	}
}

func TestPassBacksOffAfterRepeatedShortfalls(t *testing.T) {
	tests := []struct {
		period      time.Duration
		wantFreedOn []int
	}{
		// Passes 1 to 3 free, at 0, 5 and 10 minutes. The retry is an hour
		// after pass 3, at 70 minutes, which is pass 15.
		{period: 5 * time.Minute, wantFreedOn: []int{1, 2, 3, 15}},
		// Fifteen passes span 7 minutes, so no retry is due.
		{period: 30 * time.Second, wantFreedOn: []int{1, 2, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.period.String(), func(t *testing.T) {
			// The store holds 50 bytes it can't free, so every pass falls short.
			var calls []string
			c, space, clk := oneStore(t, false, &fakeStore{name: "images", used: 50, calls: &calls})
			var freedOn []int
			for pass := 1; pass <= 15; pass++ {
				before := len(calls)
				c.pass(context.Background())
				if len(calls) > before {
					freedOn = append(freedOn, pass)
				}
				clk.SetTime(clk.Now().Add(tc.period))
			}
			if !slices.Equal(freedOn, tc.wantFreedOn) {
				t.Errorf("freed on passes %v, want %v", freedOn, tc.wantFreedOn)
			}
			// A quiet pass ends the streak, so the next pressure frees right away.
			space[1] = 500
			c.pass(context.Background())
			space[1] = 100
			calls = calls[:0]
			c.pass(context.Background())
			if len(calls) != 1 {
				t.Errorf("freed on %d passes after a quiet one, want 1", len(calls))
			}
		})
	}
}

func TestPassSkipsUnsizedStore(t *testing.T) {
	fs, space := oneFS(100)
	var calls []string
	good := &fakeStore{name: "good", used: 300, reclaimable: 300, calls: &calls, space: space}
	c, _ := newCoordinator(t, fs, false, []Store{
		{Name: "unsized", Dir: "/boot",
			Used: func(context.Context) (int64, error) { return 0, errors.New("unreadable") },
			Free: func(context.Context, int64, bool) (int64, error) { calls = append(calls, "unsized"); return 0, nil }},
		{Name: "good", Dir: "/boot", Used: good.Used, Free: good.Free},
	})
	res := c.pass(context.Background())[0]
	if !slices.Equal(calls, []string{"good"}) || res.Outcome != outcomeComplete || res.Err == nil {
		t.Errorf("calls=%v outcome=%s err=%v, want [good] complete with the sizing error", calls, res.Outcome, res.Err)
	}
}

// TestPassLogs runs 15 passes per row through pass and logResult, and counts
// the lines logged at the default level, with passes 5 minutes apart. Repeated
// shortfalls and errors warn on passes 1 to 3 and then an hour after pass 3,
// on pass 15. It swaps the default logger, so tests in this package must not
// use t.Parallel.
func TestPassLogs(t *testing.T) {
	const shortfallMsg = "level=WARN msg=\"Disk GC could not free enough"
	const errorMsg = "level=WARN msg=\"Disk GC pass had errors"
	const completeMsg = "level=INFO msg=\"Disk GC pass complete"
	unsized := func(context.Context) (int64, error) { return 0, errors.New("unreadable") }
	tests := []struct {
		name string
		// setup returns the coordinator, its clock, and a func run before
		// each pass.
		setup     func(calls *[]string) (*Coordinator, *testingclock.FakePassiveClock, func())
		wantFrees int
		want      map[string]int
	}{
		{
			// The store holds more than the deficit but nothing is freeable
			// yet, as when layers are under the min age. Keep asking, and
			// throttle only the warning.
			name: "protected stores keep freeing",
			setup: func(calls *[]string) (*Coordinator, *testingclock.FakePassiveClock, func()) {
				c, _, clk := oneStore(t, false, &fakeStore{name: "images", used: 300, calls: calls})
				return c, clk, func() {}
			},
			wantFrees: 15,
			want:      map[string]int{shortfallMsg: 4},
		},
		{
			name: "repeated sizing errors on a quiet filesystem",
			setup: func(*[]string) (*Coordinator, *testingclock.FakePassiveClock, func()) {
				fs, _ := oneFS(500)
				c, clk := newCoordinator(t, fs, false, []Store{{Name: "unsized", Dir: "/boot", Used: unsized}})
				return c, clk, func() {}
			},
			want: map[string]int{errorMsg: 4},
		},
		{
			name: "statfs fails",
			setup: func(*[]string) (*Coordinator, *testingclock.FakePassiveClock, func()) {
				c, clk := newCoordinator(t, fakeFS{devices: map[string]uint64{"/boot": 1}, failSpace: true}, false,
					[]Store{{Name: "images", Dir: "/boot", Used: func(context.Context) (int64, error) { return 0, nil }}})
				return c, clk, func() {}
			},
			want: map[string]int{errorMsg: 4},
		},
		{
			// One store can't be sized and the other frees the whole deficit
			// each time. The repeated error throttles, but every completion
			// still logs.
			name: "a throttled error doesn't hide completions",
			setup: func(calls *[]string) (*Coordinator, *testingclock.FakePassiveClock, func()) {
				fs, space := oneFS(100)
				good := &fakeStore{name: "good", used: 300, reclaimable: 10000, calls: calls, space: space}
				c, clk := newCoordinator(t, fs, false, []Store{
					{Name: "unsized", Dir: "/boot", Used: unsized},
					{Name: "good", Dir: "/boot", Used: good.Used, Free: good.Free},
				})
				return c, clk, func() { space[1] = 100 } // full again
			},
			wantFrees: 15,
			want:      map[string]int{completeMsg: 15, errorMsg: 4},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			var calls []string
			c, clk, before := tc.setup(&calls)
			for range 15 {
				before()
				for _, res := range c.pass(context.Background()) {
					logResult(context.Background(), res, false)
				}
				clk.SetTime(clk.Now().Add(5 * time.Minute))
			}
			if tc.wantFrees != 0 && len(calls) != tc.wantFrees {
				t.Errorf("freed on %d of 15 passes, want %d", len(calls), tc.wantFrees)
			}
			for msg, want := range tc.want {
				if got := strings.Count(buf.String(), msg); got != want {
					t.Errorf("%q logged %d times, want %d", msg, got, want)
				}
			}
		})
	}
}

func TestPassDryRunDeletesNothing(t *testing.T) {
	var calls []string
	store := &fakeStore{name: "images", used: 300, reclaimable: 300, calls: &calls}
	c, space, _ := oneStore(t, true, store)
	res := c.pass(context.Background())[0]
	if res.Freed != 200 || res.Measured != 0 || res.Outcome != outcomeComplete {
		t.Errorf("got freed=%d measured=%d outcome=%s, want 200 0 complete", res.Freed, res.Measured, res.Outcome)
	}
	if store.reclaimable != 300 || space[1] != 100 {
		t.Errorf("dry run changed the store or the filesystem: reclaimable=%d avail=%d", store.reclaimable, space[1])
	}
}

func TestNewRejects(t *testing.T) {
	sized := func(context.Context) (int64, error) { return 0, nil }
	tests := []struct {
		name      string
		high, low int
		stores    []Store
	}{
		{name: "equal watermarks", high: 80, low: 80},
		{name: "low over high", high: 70, low: 80},
		{name: "high over 100", high: 101, low: 50},
		{name: "negative low", high: 80, low: -1},
		{name: "duplicate store names", high: 80, low: 70, stores: []Store{
			{Name: "trash", Dir: "/a", Used: sized},
			{Name: "trash", Dir: "/b", Used: sized},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(fakeFS{}, testingclock.NewFakePassiveClock(time.Time{}), tc.high, tc.low, false, tc.stores); err == nil {
				t.Error("New() = nil error, want one")
			}
		})
	}
}
