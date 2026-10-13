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

package controlapi

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	reportNode  = "node-a"
	reportOther = "node-b"
)

func reportURI(gen int32) string { return fmt.Sprintf("gs://bucket/actors/a/snap-%d", gen) }

// recorded is a Snapshot entry as pause records it: a local checkpoint on
// locality with its durable copy at storage.
func recorded(gen int32, locality string, storage ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Uuid:       fmt.Sprintf("snap-%d", gen),
		Generation: gen,
		Locality:   locality,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status: storage,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: reportURI(gen)},
		},
	}
}

func reported(gen int32) *ateapipb.SnapshotReport {
	return &ateapipb.SnapshotReport{
		Uuid:       fmt.Sprintf("snap-%d", gen),
		Generation: gen,
		Fidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:     &ateapipb.ObjectSnapshot{SnapshotUri: reportURI(gen)},
	}
}

func TestApplyActorSnapshotState(t *testing.T) {
	const (
		pending   = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		completed = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		paused    = ateapipb.ActorState_ACTOR_STATE_PAUSED
		running   = ateapipb.ActorState_ACTOR_STATE_RUNNING
	)
	tests := []struct {
		name     string
		status   *ateapipb.ActorStatus
		state    *ateapipb.ActorSnapshotState
		want     *ateapipb.ActorStatus
		wantCode codes.Code
	}{{
		name: "upload marks the local snapshot durable and keeps it local",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
		}},
	}, {
		name: "upload prunes the older durable snapshot",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(1, "", completed),
			recorded(2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
		}},
	}, {
		name: "duplicate report changes nothing",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
		}},
	}, {
		name: "stale durable report is a no-op",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(3, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(3), LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(3, reportNode, completed),
		}},
	}, {
		name: "unknown generation is refused",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, pending),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: reported(5)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "uuid mismatch is refused",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: func() *ateapipb.SnapshotReport {
			r := reported(2)
			r.Uuid = "other"
			return r
		}()},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "uri mismatch is refused",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(2), LatestDurable: func() *ateapipb.SnapshotReport {
			r := reported(2)
			r.Object.SnapshotUri = "gs://elsewhere/x"
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "detach releases a paused actor from the node",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, Snapshots: []*ateapipb.Snapshot{
			recorded(2, "", completed),
		}},
	}, {
		name: "detach of a non-durable latest snapshot is refused",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(1, "", completed),
			recorded(2, reportNode, pending),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestDurable: reported(1)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "detach keeps the assigned node of an actor that is not idle",
		status: &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(2, "", completed),
		}},
	}, {
		name: "detach from the old node leaves the new node alone",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			recorded(2, reportNode, completed),
			recorded(3, reportOther, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(2)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			recorded(2, "", completed),
			recorded(3, reportOther, pending),
		}},
	}, {
		name: "older local snapshot atelet no longer lists loses its locality",
		status: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(1, "", completed),
			recorded(2, reportNode, pending),
			recorded(3, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(3), LatestDurable: reported(1)},
		want: &ateapipb.ActorStatus{State: paused, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			recorded(1, "", completed),
			recorded(3, reportNode, pending),
		}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyActorSnapshotState(tt.status, reportNode, tt.state)
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("applyActorSnapshotState() error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyActorSnapshotState() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, tt.status, protocmp.Transform()); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
