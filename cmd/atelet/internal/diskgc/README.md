# diskgc

Package `diskgc` coordinates freeing disk space on a node. It counts what each of atelet's stores
holds on each filesystem. When a filesystem goes over its high watermark, it asks the stores to free
space down to the low watermark, cheapest data first.

This is a prototype. It's off unless `--disk-gc-period` is set. The design is tracked in #2987, and
the per-store accounting in #3820.

## Why

Each cache frees space on its own. The image cache GC watches volume watermarks. The file caches
watch byte budgets, but only when a budget is set, and by default none is. None of them knows what
the others hold, and none can tell when the disk is full of data it doesn't own. The coordinator
gives one place that sees the whole filesystem, decides how much to free, and decides which store
goes first.

## How a pass works

A pass runs every `--disk-gc-period`. It groups the stores by the filesystem their `Dir` is on, so a
store on its own disk is handled separately. For each filesystem, the pass does the following.

1. It reads statfs and asks each store what it holds. A store that can't be sized isn't asked to
   free, and its bytes count as untracked.
2. If used space is at or under the high watermark, the pass is quiet and stops.
3. It computes the deficit, which is how far used space is over the low watermark. The target is the
   deficit, capped at what the stores that can free hold. That way no store is asked for space that
   isn't its own.
4. If shortfalls have repeated and the stores hold less than the deficit, it backs off and skips
   freeing, as described below.
5. Otherwise it asks each store in order for what's left of the target, and stops once the target is
   met.
6. It reads statfs again, to check how much space actually came back.
7. It marks the pass complete when the stores freed the whole deficit. Otherwise it records a
   shortfall and names the largest class of data, which may be "untracked".

A panic in a store is recovered. That filesystem's pass is skipped and logged at error, and atelet
keeps running.

## Flags

| Flag                     | Default | Meaning                                                          |
| ------------------------ | ------- | ---------------------------------------------------------------- |
| `--disk-gc-period`       | `0`     | How often to run a pass. `0` turns the coordinator off.          |
| `--disk-gc-high-percent` | `80`    | Used percent of a filesystem that starts freeing.                |
| `--disk-gc-low-percent`  | `75`    | Used percent that freeing aims for. Must be below the high mark. |
| `--disk-gc-dry-run`      | `false` | Delete nothing. Log what each pass would free.                   |

The image cache GC still runs on its own, with its own flags. Removing its watermark half is a
planned follow-up. The file cache GC loops run only when `--sandbox-asset-cache-max-bytes` or
`--snapshot-cache-max-bytes` is set. Without a budget, the coordinator is the only thing that frees
the file caches.

## Stores today

The stores are wired up in `startDiskGC` in `cmd/atelet/diskgc.go`, in freeing order.

| Order | Name            | Used                         | Free                           | What it keeps                                         |
| ----- | --------------- | ---------------------------- | ------------------------------ | ----------------------------------------------------- |
| 1     | `image_cache`   | `imagecache.Store.CacheSize` | `imagecache.Store.EvictUnused` | Images in use, and layers under the min age           |
| 2     | `sandbox_asset` | `filecache.Store.TotalBytes` | `filecache.Store.EvictUnused`  | Entries a worker links, and entries under the min age |
| 3     | `snapshot`      | `filecache.Store.TotalBytes` | `filecache.Store.EvictUnused`  | The same, when the snapshot cache is enabled          |

Everything else on the filesystem counts as used space but belongs to no store. That includes
running actors' DurableDirs and rootfs writes, local snapshots, the image plugin cache, and data
from containerd, kubelet and the OS. Running actors' data is never freed. Local snapshots need the
snapshot tracker to take a byte target (#3101).

## Repeated shortfalls and errors

A shortfall or an error warns on its first three passes in a row. After that it logs at debug, with
a warning once an hour as a reminder.

Freeing backs off on the same cadence, but only when the stores hold less than the deficit. In that
case even emptying them couldn't fix the disk. When they hold enough, the pass keeps asking every
time, because protected entries age out or are released.

The hour is `shortfallRetryEvery`. It's a duration, not a pass count, so it means the same at any
`--disk-gc-period`. The image cache GC's reminder, every twelfth pass at its 5 minute period, also
comes to about an hour. The limit of three stays a count, because it asks whether several checks in
a row saw the same thing.

## Adding a store

A store is a `Store` value in the list passed to `New`. It has a name, a directory, and two
functions.

```go
type Store struct {
	Name string
	Dir  string
	Used func(ctx context.Context) (int64, error)
	Free func(ctx context.Context, target int64, dryRun bool) (int64, error)
}
```

### What a store must do

The pass depends on these.

- `Dir` is the directory the store writes to. Its filesystem decides which passes count and free the
  store. A store with data on more than one filesystem registers one `Store` per filesystem.
- `Name` is unique, since results are keyed by it. `New` rejects duplicates. A store registered once
  per directory puts the directory in its name, because two directories can share a filesystem.
- `Free` frees up to `target` bytes and reports how many it freed. It's never called with a target
  of zero or less. It keeps the store's own protections, such as min ages, links and pins. The
  coordinator never picks entries. If it fails partway, it returns what it freed along with the
  error.
- With `dryRun`, `Free` deletes nothing and reports what it would have freed.
- `Free` runs on the coordinator's goroutine, at the same time as the store's own work. It's safe to
  call concurrently with that work, and it returns promptly when `ctx` is done.
- A store whose data can't be freed sets `Free` to nil. It's counted, so shortfall logs can name it,
  but it's never asked to free.

### What a store should do

These make a pass accurate and cheap. A store that skips them still works.

- `Used` reports allocated bytes, since the pass compares store sizes with statfs. Sizes cap the
  target, name the largest class, and decide whether freeing backs off. A store that understates
  `Used` can make the pass back off when the stores could have fixed the disk. The file caches
  report allocated bytes. The image cache reports file sizes instead, which drift further from
  statfs for many small or sparse files.
- `Used` reads a running total, since it runs on every pass. Today the file caches walk their
  entries on each call, and the image cache reads one size file per layer. The file cache size
  metric already walks them on every collection. The coordinator adds a walk every
  `--disk-gc-period`, so a short period walks often. A new store shouldn't add another walk.

### Where it goes in the order

Put the store where its data is cheapest to lose relative to the others. The order proposed for
#2987 is below.

1. Trash awaiting deletion
2. Durable local snapshots
3. Unused image layers
4. Unlinked asset and snapshot cache entries
5. Undurable local snapshots, which have to be uploaded before they can be freed

### Example, the trash from #3756

#3756 proposes renaming actor directories into a `.trash/` directory on each mount, and deleting
them in the background. Nobody needs that data, so it goes first.

The trash needs a way to report the size of one `.trash` directory, and a way to delete entries in
it on demand. The reaper and its methods below are placeholders for whatever #3756 adds.

```go
// trashStore counts and frees one mount's .trash directory.
func trashStore(dir string, reaper *trash.Reaper) diskgc.Store {
	return diskgc.Store{
		// Unique per directory, since two mounts can share a filesystem.
		Name: "trash:" + dir,
		Dir:  dir,
		// A running total for dir, kept by the reaper as it renames and
		// deletes, so a pass doesn't walk the trash.
		Used: func(context.Context) (int64, error) { return reaper.Bytes(dir) },
		// Deletes whole entries in dir, oldest first, until target bytes
		// are gone.
		Free: func(ctx context.Context, target int64, dryRun bool) (int64, error) {
			return reaper.Reap(ctx, dir, target, dryRun)
		},
	}
}
```

Each store reads and frees only its own directory. Otherwise a full boot disk could delete trash on
another disk.

A name with the full path is fine in logs. If store names become a metric label, use a short,
bounded form instead, such as the mount's name.

In `startDiskGC`, after the existing stores are built, put one trash store per mount ahead of them.
The rest of the function is unchanged.

```go
var trashStores []diskgc.Store
for _, dir := range trashDirs { // one .trash per mount
	trashStores = append(trashStores, trashStore(dir, reaper))
}
stores = append(trashStores, stores...)
```

The trash has no protections, so `Reap` can delete anything in it. The background reaper keeps
running on its own, as the cache GC loops do. The coordinator only makes it delete sooner when the
disk is full.

### Tests for a new store

- Test `Used` and `Free` in the store's own package, including the dry run and its protections.
- The coordinator's tests use fake stores and don't need a change for a new store. Add a row to
  `TestPass` only if the new store needs a behavior the coordinator doesn't have yet.

## Testing

`diskgc_test.go` uses a fake filesystem, fake stores and a fake clock. It covers grouping by
filesystem, order, the target cap, shortfalls, backoff, the dry run, panics, log throttling, and the
checks in `New`.

On a cluster, set the watermarks just under a node's current used percent, with
`--disk-gc-period=30s` and `--log-level=debug`. A small boot disk makes one percent small, so the
caches can cover the deficit. Then check the `Disk GC` log lines for the deficit, target, freed and
measured bytes, and the outcome. Restore atelet's flags afterwards.
