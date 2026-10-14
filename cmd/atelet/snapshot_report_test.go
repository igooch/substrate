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
	"strings"
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
// for every call.
type fakeReporter struct {
	// block, when set, holds each call until it is closed.
	block chan struct{}

	mu   sync.Mutex
	reqs []*ateapipb.ReportActorSnapshotStateRequest
	err  error
}

func (f *fakeReporter) ReportActorSnapshotState(ctx context.Context, req *ateapipb.ReportActorSnapshotStateRequest, _ ...grpc.CallOption) (*ateapipb.ReportActorSnapshotStateResponse, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
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

// A background detach deletes the local snapshots and reports that atelet no
// longer holds the actor locally.
func TestTrackerReportsDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	tr, reporter := newReportingTracker(t, store)
	snapDir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	detachNow(t, tr)
	if _, err := os.Stat(snapDir); !os.IsNotExist(err) {
		t.Errorf("local snapshot still on disk after the detach: %v", err)
	}
	var detach *ateapipb.ReportActorSnapshotStateRequest
	eventually(t, "the detach is reported", func() bool {
		for _, r := range reporter.reports() {
			if r.GetState().GetLatestLocal() == nil {
				detach = r
			}
		}
		return detach != nil
	})
	if got := detach.GetState().GetLatestDurable().GetUuid(); got != "s1" {
		t.Errorf("detach report latest_durable = %q, want s1", got)
	}
}

// A background detach does not wait for its report. A report the control
// plane holds back, for example behind a resume that holds the actor's lease
// and waits for this detach, cannot hold the detach up.
func TestTrackerDetachDoesNotWaitForReport(t *testing.T) {
	store := newFakeSnapshotStore()
	tr, reporter := newReportingTracker(t, store)
	unblock := make(chan struct{})
	defer close(unblock)
	reporter.block = unblock
	snapDir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	detachNow(t, tr)
	if _, err := os.Stat(snapDir); !os.IsNotExist(err) {
		t.Errorf("local snapshot still on disk while the report is blocked: %v", err)
	}
	tr.mu.Lock()
	_, err := tr.waitForDetachLocked(t.Context(), trackedActorUID)
	tr.mu.Unlock()
	if err != nil {
		t.Errorf("waiting for the detach while its report is blocked: %v", err)
	}
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

// A change made before atelet can reach the control plane is reported once it
// can.
func TestTrackerReportsOnceReporterIsSet(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.identify(trackedActorUID, "ate-demo", "counter")
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, nil)

	reporter := &fakeReporter{}
	tr.setReporter(reporter)
	eventually(t, "the upload is reported", func() bool { return len(reporter.reports()) == 1 })
	if got := reporter.reports()[0].GetState().GetLatestDurable().GetUuid(); got != "s1" {
		t.Errorf("latest_durable = %q, want s1", got)
	}
}

// A report atelet owed when it stopped is sent by the next atelet.
func TestTrackerReportsOwedAfterRestart(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.identify(trackedActorUID, "ate-demo", "counter")
	tr.commitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, nil)

	restarted := newTestTrackerOnNode(t, newFakeSnapshotStore())
	if err := restarted.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	restarted.start()
	reporter := &fakeReporter{}
	restarted.setReporter(reporter)
	eventually(t, "the owed report is sent", func() bool { return len(reporter.reports()) == 1 })
	got := reporter.reports()[0]
	if got.GetActor().GetName() != "counter" || got.GetState().GetLatestDurable().GetUuid() != "s1" {
		t.Errorf("report = %v, want counter with s1 durable", got)
	}

	// Once sent, nothing is owed: a third atelet reports nothing.
	eventually(t, "the sent report is no longer owed", func() bool {
		data, err := os.ReadFile(ateletpath.ActorSnapshotStateFile(trackedActorUID))
		return err == nil && !strings.Contains(string(data), "reportOwed")
	})
}
