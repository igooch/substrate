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

// Package diskgc accounts for the data atelet keeps on each filesystem, and
// frees space on a full one by asking its stores to free bytes, cheapest first.
package diskgc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"k8s.io/utils/clock"
)

// Store is data on the node's disk that the coordinator counts, and may free.
type Store struct {
	// Name names the store in logs. It must be unique among the stores.
	Name string
	// Dir is the directory the store writes to. Its filesystem decides which
	// passes count and free it.
	Dir string
	// Used reports the bytes the store holds.
	Used func(ctx context.Context) (int64, error)
	// Free frees up to target bytes, keeping the store's own protections, and
	// reports how many it freed. With dryRun it deletes nothing and reports
	// what it would free. Nil means the store is only counted, as for data that
	// can't be freed.
	Free func(ctx context.Context, target int64, dryRun bool) (int64, error)
}

// FS reports which filesystem a directory is on and how full it is.
type FS interface {
	// Device returns the ID of the filesystem dir is on.
	Device(dir string) (uint64, error)
	// Space returns the total and available bytes of the filesystem dir is on.
	Space(dir string) (total, avail uint64, err error)
}

// Outcomes of one filesystem's pass.
const (
	outcomeQuiet     = "quiet"     // the filesystem was under its high watermark
	outcomeComplete  = "complete"  // the stores freed what the pass targeted
	outcomeShortfall = "shortfall" // they freed less, since the rest is too young, in use, or not held by a store
	outcomeSkipped   = "skipped"   // the filesystem could not be measured, or a store panicked
)

// After shortfallWarnLimit consecutive shortfalls (or passes with errors) on a
// filesystem, a pass warns only once every shortfallRetryEvery, as a reminder.
// Freeing backs off on the same cadence only when the stores hold less than
// the deficit. Freeing everything couldn't fix the disk, and freeing on every
// pass would empty the caches as fast as they refill. When they hold enough,
// the pass keeps freeing every time, since protected entries age out or are
// released. The reminder is a duration so it means the same at any period.
const (
	shortfallWarnLimit  = 3
	shortfallRetryEvery = time.Hour
)

// result is what one pass found and did on one filesystem.
type result struct {
	// Dir is the first store directory on the filesystem, to name it in logs.
	Dir string
	// Total and Used are the filesystem's size and used bytes.
	Total, Used int64
	// Stores maps each store on the filesystem to the bytes it held at the
	// start of the pass. Used minus their sum is data no store accounts for.
	Stores map[string]int64
	// Deficit is how much the filesystem is over its low watermark. Target is
	// what the pass asked the stores for, the deficit capped at what the
	// freeable stores hold, so they're never asked for space that isn't theirs.
	Deficit, Target int64
	// BackedOff is set when the pass skipped freeing after repeated
	// shortfalls the stores couldn't fix.
	BackedOff bool
	// throttled and errThrottled are set when a repeated shortfall or a
	// repeated error is lowered to debug. Unset means log at the normal level.
	throttled, errThrottled bool
	// Freed is what the stores reported freeing. Measured is how much the
	// filesystem's used space dropped by the end of the pass. Measured is lower
	// when a freed file is still open somewhere, and it moves either way when
	// other writers are active.
	Freed, Measured int64
	Outcome         string
	Err             error
	// stack is set when the pass panicked.
	stack string
}

// Coordinator runs passes over its stores, grouped by filesystem.
type Coordinator struct {
	fs      FS
	highPct int
	lowPct  int
	dryRun  bool
	stores  []Store
	// shortfalls and errs track consecutive shortfalls and passes with errors
	// per filesystem, keyed by the group's dir.
	shortfalls map[string]*streak
	errs       map[string]*streak
	// clock times the reminders.
	clock clock.PassiveClock
}

// New returns a Coordinator that frees a filesystem over highPct used down to
// lowPct used. stores are in freeing order, cheapest data first, and their
// names must be unique. With dryRun it deletes nothing and logs what it would
// free.
func New(fs FS, clk clock.PassiveClock, highPct, lowPct int, dryRun bool, stores []Store) (*Coordinator, error) {
	if err := CheckWatermarks(highPct, lowPct); err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, store := range stores {
		if names[store.Name] {
			return nil, fmt.Errorf("two stores are named %q; results are keyed by name", store.Name)
		}
		names[store.Name] = true
	}
	return &Coordinator{fs: fs, clock: clk, highPct: highPct, lowPct: lowPct, dryRun: dryRun, stores: stores, shortfalls: map[string]*streak{}, errs: map[string]*streak{}}, nil
}

// CheckWatermarks reports an error unless 0 <= lowPct < highPct <= 100.
func CheckWatermarks(highPct, lowPct int) error {
	if lowPct < 0 || highPct > 100 || lowPct >= highPct {
		return fmt.Errorf("watermarks must satisfy 0 <= low < high <= 100, got low=%d high=%d", lowPct, highPct)
	}
	return nil
}

// streak is a run of consecutive shortfalls or errors on one filesystem.
type streak struct {
	count int
	// lastLoud is when a pass in the streak last warned, and for shortfalls,
	// last freed.
	lastLoud time.Time
}

// fsGroup is the stores on one filesystem, in freeing order.
type fsGroup struct {
	dir    string
	stores []Store
}

// groups splits the stores by filesystem, keeping their order within each. A
// store whose filesystem can't be found is left out of this pass.
func (c *Coordinator) groups(ctx context.Context) []fsGroup {
	var groups []fsGroup
	groupIndex := map[uint64]int{}
	for _, store := range c.stores {
		device, err := c.fs.Device(store.Dir)
		if err != nil {
			slog.WarnContext(ctx, "Disk GC: cannot find a store's filesystem; leaving it out of this pass",
				slog.String("store", store.Name), slog.String("dir", store.Dir), slog.Any("err", err))
			continue
		}
		pos, ok := groupIndex[device]
		if !ok {
			pos = len(groups)
			groupIndex[device] = pos
			groups = append(groups, fsGroup{dir: store.Dir})
		}
		groups[pos].stores = append(groups[pos].stores, store)
	}
	return groups
}

// pass accounts for and, where needed, frees every filesystem the stores are
// on.
func (c *Coordinator) pass(ctx context.Context) []result {
	var results []result
	for _, group := range c.groups(ctx) {
		results = append(results, c.reclaimFS(ctx, group))
	}
	return results
}

// reclaimFS measures one filesystem and, when it's over the high watermark,
// asks its stores to free space. It recovers from a panic in a store, so a bug
// in one store can't take atelet down.
func (c *Coordinator) reclaimFS(ctx context.Context, group fsGroup) (res result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			res.Dir, res.Outcome = group.dir, outcomeSkipped
			res.Err = fmt.Errorf("panic in pass: %v", recovered)
			res.stack = string(debug.Stack())
		}
	}()
	var errs []error
	res, errs = c.measure(ctx, group)
	switch {
	case res.Outcome == outcomeSkipped:
	case res.Used*100 <= res.Total*int64(c.highPct):
		delete(c.shortfalls, group.dir)
		res.Outcome = outcomeQuiet
	default:
		errs = append(errs, c.free(ctx, group, &res)...)
	}
	res.Err = errors.Join(errs...)
	c.throttleErr(group.dir, &res)
	return res
}

// measure reads the filesystem's size and what each store holds. A failed
// statfs skips the pass. A store that can't be sized is left out of res.Stores.
func (c *Coordinator) measure(ctx context.Context, group fsGroup) (result, []error) {
	res := result{Dir: group.dir, Stores: map[string]int64{}}
	total, avail, err := c.fs.Space(group.dir)
	if err != nil {
		res.Outcome = outcomeSkipped
		return res, []error{err}
	}
	res.Total, res.Used = int64(total), int64(total-avail)
	var errs []error
	for _, store := range group.stores {
		used, err := store.Used(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("sizing %s: %w", store.Name, err))
			continue
		}
		res.Stores[store.Name] = used
	}
	return res, errs
}

// free asks the stores for the filesystem's deficit, then records how much
// space came back and the outcome. It backs off after repeated shortfalls
// that even emptying the stores couldn't fix.
func (c *Coordinator) free(ctx context.Context, group fsGroup, res *result) []error {
	res.Deficit = res.Used - res.Total*int64(c.lowPct)/100
	freeable := freeableBytes(group.stores, res.Stores)
	res.Target = min(res.Deficit, freeable)
	reminder := c.remind(c.shortfalls[group.dir])
	var errs []error
	if freeable < res.Deficit && !reminder {
		res.BackedOff = true
	} else {
		errs = askStores(ctx, group.stores, res, c.dryRun)
	}
	if err := c.measureFreed(group.dir, res); err != nil {
		errs = append(errs, err)
	}
	c.recordOutcome(group.dir, res, reminder)
	return errs
}

// freeableBytes sums what the sized stores that can free hold.
func freeableBytes(stores []Store, sizes map[string]int64) int64 {
	var total int64
	for _, store := range stores {
		if store.Free != nil {
			total += sizes[store.Name]
		}
	}
	return total
}

// askStores asks each store in order for what's left of res.Target, until the
// target is met. A store that couldn't be sized isn't asked, failing toward
// keeping its data, as the per-store loops do.
func askStores(ctx context.Context, stores []Store, res *result, dryRun bool) []error {
	var errs []error
	for _, store := range stores {
		if res.Freed >= res.Target {
			break
		}
		if _, sized := res.Stores[store.Name]; store.Free == nil || !sized {
			continue
		}
		freed, err := store.Free(ctx, res.Target-res.Freed, dryRun)
		res.Freed += freed
		if err != nil {
			errs = append(errs, fmt.Errorf("freeing %s: %w", store.Name, err))
		}
	}
	return errs
}

// measureFreed sets res.Measured to how much the filesystem's used space dropped
// since measure.
func (c *Coordinator) measureFreed(dir string, res *result) error {
	total, avail, err := c.fs.Space(dir)
	if err != nil {
		return fmt.Errorf("measuring after freeing: %w", err)
	}
	res.Measured = res.Used - int64(total-avail)
	return nil
}

// recordOutcome marks the pass complete when the stores freed the whole
// deficit, and otherwise counts the shortfall, throttling its warning unless
// this pass is a reminder.
func (c *Coordinator) recordOutcome(dir string, res *result, reminder bool) {
	if res.Freed >= res.Deficit {
		delete(c.shortfalls, dir)
		res.Outcome = outcomeComplete
		return
	}
	res.Outcome = outcomeShortfall
	res.throttled = !reminder
	c.extend(c.shortfalls, dir, reminder)
}

// throttleErr counts consecutive passes with errors on dir, and sets
// res.errThrottled except on the first few and then every reminder.
func (c *Coordinator) throttleErr(dir string, res *result) {
	if res.Err == nil {
		delete(c.errs, dir)
		return
	}
	reminder := c.remind(c.errs[dir])
	res.errThrottled = !reminder
	c.extend(c.errs, dir, reminder)
}

// remind reports whether the next pass in a streak should act and warn as
// usual. It does for the first few passes, and then once every
// shortfallRetryEvery.
func (c *Coordinator) remind(s *streak) bool {
	return s == nil || s.count < shortfallWarnLimit || c.clock.Since(s.lastLoud) >= shortfallRetryEvery
}

// extend adds a pass to dir's streak in streaks, noting when it was loud.
func (c *Coordinator) extend(streaks map[string]*streak, dir string, loud bool) {
	s := streaks[dir]
	if s == nil {
		s = &streak{}
		streaks[dir] = s
	}
	s.count++
	if loud {
		s.lastLoud = c.clock.Now()
	}
}

// largest names the store holding the most bytes, or "untracked" when data no
// store accounts for is larger than any store.
func (r result) largest() string {
	untracked := r.Used
	for _, size := range r.Stores {
		untracked -= size
	}
	largestName, largestSize := "untracked", untracked
	for name, size := range r.Stores {
		if size > largestSize {
			largestName, largestSize = name, size
		}
	}
	return largestName
}

// Run runs a pass every period until ctx is done, logging each filesystem's
// result. Don't call it more than once at a time.
func (c *Coordinator) Run(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		for _, res := range c.pass(ctx) {
			logResult(ctx, res, c.dryRun)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func logResult(ctx context.Context, res result, dryRun bool) {
	attrs := []any{
		slog.String("dir", res.Dir), slog.String("outcome", res.Outcome),
		slog.Int64("used_bytes", res.Used), slog.Int64("total_bytes", res.Total),
		slog.Int64("deficit_bytes", res.Deficit), slog.Int64("target_bytes", res.Target),
		slog.Int64("freed_bytes", res.Freed), slog.Bool("dry_run", dryRun),
		slog.Int64("measured_freed_bytes", res.Measured),
		slog.Any("stores", res.Stores),
	}
	if res.Outcome == outcomeShortfall {
		attrs = append(attrs, slog.String("largest", res.largest()))
	}
	attrs = append(attrs, slog.Bool("backed_off", res.BackedOff))
	if res.stack != "" {
		slog.ErrorContext(ctx, "Disk GC pass panicked; skipping this filesystem",
			append(attrs, slog.Any("err", res.Err), slog.String("stack", res.stack))...)
		return
	}
	// The outcome and any error log separately, so a throttled error can't
	// hide an eviction, and neither cadence silences the other.
	if res.Err != nil {
		slog.Log(ctx, levelFor(res.errThrottled), "Disk GC pass had errors", append(attrs, slog.Any("err", res.Err))...)
	}
	switch res.Outcome {
	case outcomeShortfall:
		slog.Log(ctx, levelFor(res.throttled), "Disk GC could not free enough; the rest is not freeable now, being too young, in use, or not held by a store", attrs...)
	case outcomeComplete:
		slog.InfoContext(ctx, "Disk GC pass complete", attrs...)
	}
}

// levelFor is warn, or debug when throttled.
func levelFor(throttled bool) slog.Level {
	if throttled {
		return slog.LevelDebug
	}
	return slog.LevelWarn
}
