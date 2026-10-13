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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// snapshotRecord is a snapshotRef as the snapshot state file keeps it.
type snapshotRecord struct {
	UUID       string `json:"uuid"`
	Generation int32  `json:"generation"`
	// Fidelity is the ateattr label: empty when unknown.
	Fidelity   string    `json:"fidelity,omitempty"`
	URI        string    `json:"uri"`
	CommitTime time.Time `json:"commitTime,omitzero"`
	UploadTime time.Time `json:"uploadTime,omitzero"`
}

func recordOf(r *snapshotRef) *snapshotRecord {
	if r == nil {
		return nil
	}
	rec := &snapshotRecord{
		UUID:       r.uuid,
		Generation: r.generation,
		URI:        r.uri,
		CommitTime: r.commitTime,
		UploadTime: r.uploadTime,
	}
	if r.fidelity != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		rec.Fidelity = ateattr.SnapshotFidelityValue(r.fidelity)
	}
	return rec
}

func (rec *snapshotRecord) ref() *snapshotRef {
	if rec == nil {
		return nil
	}
	return &snapshotRef{
		uuid:       rec.UUID,
		generation: rec.Generation,
		fidelity:   fidelityFromLabel(rec.Fidelity),
		uri:        rec.URI,
		commitTime: rec.CommitTime,
		uploadTime: rec.UploadTime,
	}
}

// actorSnapshotsRecord is the content of an actor's snapshot state file: the
// part of actorSnapshots that outlives an atelet restart.
type actorSnapshotsRecord struct {
	// Atespace and ActorName name the actor in reports to the control plane.
	Atespace           string          `json:"atespace,omitempty"`
	ActorName          string          `json:"actorName,omitempty"`
	Newest             *snapshotRecord `json:"newest,omitempty"`
	NewestByCheckpoint bool            `json:"newestByCheckpoint,omitempty"`
	LatestLocal        *snapshotRecord `json:"latestLocal,omitempty"`
	// Uploading is the snapshot whose upload was in flight. A restart ends
	// the upload, so a loaded state counts it as not durable.
	Uploading      *snapshotRecord `json:"uploading,omitempty"`
	LatestDurable  *snapshotRecord `json:"latestDurable,omitempty"`
	UndurableSince time.Time       `json:"undurableSince,omitzero"`
	RetryAt        time.Time       `json:"retryAt,omitzero"`
	DetachDue      time.Time       `json:"detachDue,omitzero"`
	Running        bool            `json:"running,omitempty"`
}

func (a *actorSnapshots) record() *actorSnapshotsRecord {
	return &actorSnapshotsRecord{
		Atespace:           a.atespace,
		ActorName:          a.actorName,
		Newest:             recordOf(a.newest),
		NewestByCheckpoint: a.newestByCheckpoint,
		LatestLocal:        recordOf(a.latestLocal),
		Uploading:          recordOf(a.uploading()),
		LatestDurable:      recordOf(a.latestDurable),
		UndurableSince:     a.undurableSince,
		RetryAt:            a.retryAt,
		DetachDue:          a.detachAt,
		Running:            a.running,
	}
}

// actorSnapshots returns the actor's entry as rec describes it. No upload or
// detach is in progress, and the actor is not queued.
func (rec *actorSnapshotsRecord) actorSnapshots(actorUID string) *actorSnapshots {
	a := &actorSnapshots{
		actorUID:           actorUID,
		atespace:           rec.Atespace,
		actorName:          rec.ActorName,
		newest:             rec.Newest.ref(),
		newestByCheckpoint: rec.NewestByCheckpoint,
		latestLocal:        rec.LatestLocal.ref(),
		latestDurable:      rec.LatestDurable.ref(),
		undurableSince:     rec.UndurableSince,
		retryAt:            rec.RetryAt,
		detachAt:           rec.DetachDue,
		running:            rec.Running,
	}
	if up := rec.Uploading.ref(); up != nil && !a.durable(up) {
		if a.undurableSince.IsZero() || up.commitTime.Before(a.undurableSince) {
			a.undurableSince = up.commitTime
		}
	}
	return a
}

// saveLocked writes what atelet tracks of the actor to its snapshot state
// file. An actor that is forgotten, or whose directory is gone, is not
// written. Best-effort: if writing fails, atelet carries on from memory, and a
// restart loses what changed.
func (t *snapshotTracker) saveLocked(a *actorSnapshots) {
	if t.actors[a.actorUID] != a {
		return
	}
	if _, err := os.Stat(ateletpath.ActorPath(a.actorUID)); err != nil {
		return
	}
	data, err := json.Marshal(a.record())
	if err == nil {
		err = writeFileAtomic(ateletpath.ActorSnapshotStateFile(a.actorUID), data, 0o600)
	}
	if err != nil {
		slog.WarnContext(t.ctx, "Failed to save the actor's snapshot state", slog.String("actorUID", a.actorUID), slog.Any("err", err))
	}
}

// removeSnapshotState deletes the actor's snapshot state file. A missing file
// is not an error.
func removeSnapshotState(actorUID string) error {
	if err := os.Remove(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("while deleting the actor's snapshot state: %w", err)
	}
	return nil
}

// load restores, from their snapshot state files, what atelet tracked of the
// actors on this node before it restarted, and queues their uploads and
// detaches. An upload in flight at the restart is due again; a detach is due
// when it was, or straight away if that has passed. A file that cannot be read
// is skipped: atelet then knows nothing of that actor, and PromoteSnapshot
// still finds its snapshot on disk or in object storage.
func (t *snapshotTracker) load() error {
	entries, err := os.ReadDir(nodepath.ActorsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while listing actor directories: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		actorUID := entry.Name()
		data, err := os.ReadFile(ateletpath.ActorSnapshotStateFile(actorUID))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		var rec actorSnapshotsRecord
		if err == nil {
			err = json.Unmarshal(data, &rec)
		}
		if err != nil {
			slog.WarnContext(t.ctx, "Skipping an unreadable snapshot state file", slog.String("actorUID", actorUID), slog.Any("err", err))
			continue
		}
		a := rec.actorSnapshots(actorUID)
		if a.detachAt.IsZero() && !a.running && a.latestLocal != nil {
			a.detachAt = t.clock.Now().Add(t.detachDelay)
		}
		t.actors[actorUID] = a
		t.requeueLocked(a)
		t.saveLocked(a)
	}
	return nil
}
