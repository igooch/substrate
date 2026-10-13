// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// fakeReporter records the reports it receives. err, when set, is returned
// for every call. onReport, when set, runs during each call.
type fakeReporter struct {
	mu       sync.Mutex
	reqs     []*ateapipb.ReportActorSnapshotStateRequest
	err      error
	onReport func(*ateapipb.ReportActorSnapshotStateRequest)
}

func (f *fakeReporter) ReportActorSnapshotState(_ context.Context, req *ateapipb.ReportActorSnapshotStateRequest, _ ...grpc.CallOption) (*ateapipb.ReportActorSnapshotStateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	if f.onReport != nil {
		f.onReport(req)
	}
	return &ateapipb.ReportActorSnapshotStateResponse{}, f.err
}

func (f *fakeReporter) reports() []*ateapipb.ReportActorSnapshotStateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapipb.ReportActorSnapshotStateRequest(nil), f.reqs...)
}

func newReportingTracker(t *testing.T, store snapshotStore) (*snapshotTracker, *fakeReporter) {
	t.Helper()
	tr := newTestTracker(t, store)
	reporter := &fakeReporter{}
	tr.setReporter(reporter)
	tr.identify(trackedActorUID, "ate-demo", "counter")
	return tr, reporter
}

// A background upload is reported with the state it leaves: the snapshot is
// both durable and still local.
func TestTrackerReportsBackgroundUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr, reporter := newReportingTracker(t, store)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, nil)
	eventually(t, "the upload is reported", func() bool { return len(reporter.reports()) == 1 })

	got := reporter.reports()[0]
	if got.GetActor().GetAtespace() != "ate-demo" || got.GetActor().GetName() != "counter" || got.GetActorUid() != trackedActorUID {
		t.Errorf("report names %v (uid %q), want ate-demo/counter (%q)", got.GetActor(), got.GetActorUid(), trackedActorUID)
	}
	st := got.GetState()
	if st.GetLatestDurable().GetUuid() != "s1" || st.GetLatestLocal().GetUuid() != "s1" {
		t.Errorf("reported state = %v, want s1 durable and local", st)
	}
	if st.GetLatestDurable().GetFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("latest_durable fidelity = %v, want MEMORY", st.GetLatestDurable().GetFidelity())
	}
}

// A background detach reports before it deletes the local snapshots, so the
// control plane stops sending local restores here first.
func TestTrackerReportsDetachBeforeDeleting(t *testing.T) {
	store := newFakeSnapshotStore()
	tr, reporter := newReportingTracker(t, store)
	snapDir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "checkpoint.img"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var filesAtReport bool
	reporter.onReport = func(req *ateapipb.ReportActorSnapshotStateRequest) {
		if req.GetState().GetLatestLocal() == nil {
			_, err := os.Stat(snapDir)
			filesAtReport = err == nil
		}
	}
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	detachNow(t, tr)
	var detach *ateapipb.ReportActorSnapshotStateRequest
	for _, r := range reporter.reports() {
		if r.GetState().GetLatestLocal() == nil {
			detach = r
		}
	}
	if detach == nil {
		t.Fatalf("reports = %v, want one for the detach", reporter.reports())
	}
	if detach.GetState().GetLatestDurable().GetUuid() != "s1" {
		t.Errorf("detach report latest_durable = %v, want s1", detach.GetState().GetLatestDurable())
	}
	if !filesAtReport {
		t.Error("the local snapshot was deleted before the detach was reported")
	}
	if _, err := os.Stat(snapDir); !os.IsNotExist(err) {
		t.Errorf("local snapshot still on disk after the detach: %v", err)
	}
}

// A detach whose report the control plane does not accept in time still
// deletes the local snapshots: the snapshot is durable by then.
func TestTrackerDetachDeletesWhenReportFails(t *testing.T) {
	store := newFakeSnapshotStore()
	tr, reporter := newReportingTracker(t, store)
	tr.reportTimeout = 50 * time.Millisecond
	reporter.err = status.Error(codes.Unavailable, "control plane down")
	snapDir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	detachNow(t, tr)
	eventually(t, "the local snapshot is deleted", func() bool {
		_, err := os.Stat(snapDir)
		return os.IsNotExist(err)
	})
}

// A refusal ends the retries: retrying cannot change the control plane's
// answer.
func TestTrackerStopsReportingWhenRefused(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			tr, reporter := newReportingTracker(t, store)
			reporter.err = status.Error(code, "refused")
			tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

			uploadNow(t, tr, store, nil)
			eventually(t, "the upload is reported", func() bool { return len(reporter.reports()) == 1 })
			stepTo(tr, tr.clock.Now().Add(2*maxReportRetryDelay))
			time.Sleep(50 * time.Millisecond)
			if got := len(reporter.reports()); got != 1 {
				t.Errorf("got %d reports, want 1: a refused report is not retried", got)
			}
		})
	}
}

// A transient failure is retried with the state as it is at the retry.
func TestTrackerRetriesTransientReportFailure(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr, reporter := newReportingTracker(t, store)
	reporter.err = status.Error(codes.Aborted, "lease held")
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, nil)
	eventually(t, "the upload is reported", func() bool { return len(reporter.reports()) == 1 })
	reporter.mu.Lock()
	reporter.err = nil
	reporter.mu.Unlock()
	eventually(t, "the report is retried", func() bool {
		stepTo(tr, tr.clock.Now().Add(reportRetryDelay))
		return len(reporter.reports()) == 2
	})
}

// An actor no Checkpoint or Restore has named is not reported: the control
// plane could not find it.
func TestTrackerDoesNotReportUnnamedActor(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	reporter := &fakeReporter{}
	tr.setReporter(reporter)
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, nil)
	time.Sleep(50 * time.Millisecond)
	if got := reporter.reports(); len(got) != 0 {
		t.Errorf("reports = %v, want none for an unnamed actor", got)
	}
}
