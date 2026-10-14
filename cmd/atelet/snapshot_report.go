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
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	reportRetryDelay    = time.Second
	maxReportRetryDelay = time.Minute
)

// snapshotReporter sends an actor's snapshot state to the control plane.
// ateapipb.WorkerServiceClient satisfies it.
type snapshotReporter interface {
	ReportActorSnapshotState(ctx context.Context, in *ateapipb.ReportActorSnapshotStateRequest, opts ...grpc.CallOption) (*ateapipb.ReportActorSnapshotStateResponse, error)
}

// setReporter sets where the tracker reports actors' snapshot state, and
// starts reporting what is owed: changes made before it was set, and changes
// a previous atelet never reported.
func (t *snapshotTracker) setReporter(r snapshotReporter) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reporter = r
	for _, a := range t.actors {
		if a.reportSent < a.reportWanted && !a.reporting {
			a.reporting = true
			t.workersDone.Go(func() { t.reportLoop(t.ctx, a) })
		}
	}
}

// identify records the actor's name, for reports about it.
func (t *snapshotTracker) identify(actorUID, atespace, actorName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.actorLocked(actorUID)
	if a.atespace != atespace || a.actorName != actorName {
		a.atespace, a.actorName = atespace, actorName
		t.saveLocked(a)
	}
}

// reportAsync reports the actor's state in the background, retrying until the
// control plane accepts or refuses it. Each attempt sends the state as it is
// then, so a report that a later change overtakes is never resent. The owed
// report is saved, so a restarted atelet sends it.
func (t *snapshotTracker) reportAsync(a *actorSnapshots) {
	t.mu.Lock()
	a.reportWanted++
	t.saveLocked(a)
	start := !a.reporting && t.reporter != nil
	if start {
		a.reporting = true
	}
	t.mu.Unlock()
	if start {
		t.workersDone.Go(func() { t.reportLoop(t.ctx, a) })
	}
}

func (t *snapshotTracker) reportLoop(ctx context.Context, a *actorSnapshots) {
	delay := reportRetryDelay
	for {
		t.mu.Lock()
		if t.actors[a.actorUID] != a || a.reportSent >= a.reportWanted || ctx.Err() != nil || t.reporter == nil {
			a.reporting = false
			t.mu.Unlock()
			return
		}
		want := a.reportWanted
		t.mu.Unlock()

		if done := t.reportOnce(ctx, a); done {
			t.mu.Lock()
			if want > a.reportSent {
				a.reportSent = want
				if a.reportSent >= a.reportWanted {
					t.saveLocked(a)
				}
			}
			t.mu.Unlock()
			delay = reportRetryDelay
			continue
		}
		select {
		case <-ctx.Done():
		case <-t.clock.After(delay):
		}
		delay = min(2*delay, maxReportRetryDelay)
	}
}

// reportOnce sends the actor's current state once. It reports whether to stop
// trying: the control plane accepted the report or refused it for good.
func (t *snapshotTracker) reportOnce(ctx context.Context, a *actorSnapshots) bool {
	t.mu.Lock()
	reporter := t.reporter
	req := &ateapipb.ReportActorSnapshotStateRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: a.atespace, Name: a.actorName},
		ActorUid: a.actorUID,
		State:    ateapiSnapshotState(t.stateLocked(a)),
	}
	t.mu.Unlock()
	if reporter == nil {
		return false
	}
	if req.GetActor().GetName() == "" {
		// No Checkpoint or Restore named the actor, so the control plane
		// could not find it.
		return true
	}

	_, err := reporter.ReportActorSnapshotState(ctx, req)
	attrs := []any{slog.String("actorUID", a.actorUID), slog.Bool("detached", req.GetState().GetLatestLocal() == nil)}
	switch status.Code(err) {
	case codes.OK:
		slog.InfoContext(ctx, "Reported actor snapshot state", attrs...)
		return true
	case codes.NotFound:
		// The control plane no longer places the actor here.
		slog.InfoContext(ctx, "Control plane no longer tracks actor on this node; stopped reporting", append(attrs, slog.Any("err", err))...)
		return true
	case codes.FailedPrecondition, codes.InvalidArgument:
		slog.ErrorContext(ctx, "Control plane refused actor snapshot state report", append(attrs, slog.Any("err", err))...)
		return true
	default:
		slog.WarnContext(ctx, "Failed to report actor snapshot state; will retry", append(attrs, slog.Any("err", err))...)
		return false
	}
}

// ateapiSnapshotState converts atelet's state for the control plane, which
// mirrors it with the snapshot fields inline.
func ateapiSnapshotState(st *ateletpb.ActorSnapshotState) *ateapipb.ActorSnapshotState {
	return &ateapipb.ActorSnapshotState{
		LatestLocal:   ateapiSnapshotReport(st.GetLatestLocal()),
		Uploading:     ateapiSnapshotReport(st.GetUploading()),
		LatestDurable: ateapiSnapshotReport(st.GetLatestDurable()),
		UploadDueTime: st.GetUploadDueTime(),
	}
}

func ateapiSnapshotReport(r *ateletpb.SnapshotReport) *ateapipb.SnapshotReport {
	if r == nil {
		return nil
	}
	return &ateapipb.SnapshotReport{
		Uuid:                r.GetSnapshot().GetSnapshotUuid(),
		Generation:          r.GetSnapshot().GetGeneration(),
		Fidelity:            ateapipb.SnapshotFidelity(r.GetSnapshot().GetFidelity()),
		Object:              &ateapipb.ObjectSnapshot{SnapshotUri: r.GetSnapshot().GetObject().GetSnapshotUri()},
		LocalCommitTime:     r.GetLocalCommitTime(),
		DurableCompleteTime: r.GetDurableCompleteTime(),
	}
}
