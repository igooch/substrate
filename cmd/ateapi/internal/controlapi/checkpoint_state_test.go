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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ateletReported is reported(t, gen) as atelet returns it.
func ateletReported(t *testing.T, gen int32) *ateletpb.SnapshotReport {
	t.Helper()
	return &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
		SnapshotUuid: snapName(gen),
		Generation:   gen,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: ownedURI(t, reportActorUID, gen)},
	}}
}

func TestApplyCheckpointState(t *testing.T) {
	const (
		pending    = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		completed  = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		suspended  = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		suspending = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
	)
	tests := []struct {
		name     string
		status   *ateapipb.ActorStatus
		state    *ateletpb.ActorSnapshotState
		want     *ateapipb.ActorStatus
		wantCode codes.Code
	}{{
		name: "an immediate checkpoint marks its snapshot COMPLETED",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: ateletReported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a deferred checkpoint changes nothing",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, ""),
		}},
		state: &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, ""),
		}},
	}, {
		name: "a deferred checkpoint records an older upload that finished",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, reportNode, pending),
			localOnly(2, ""),
		}},
		state: &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2), LatestDurable: ateletReported(t, 1)},
		want: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, reportNode, completed),
			localOnly(2, ""),
		}},
	}, {
		name: "no local snapshot is not a detach",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: ateletReported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		// The control plane doesn't send generations to atelet yet, so atelet
		// returns generation 0.
		name: "a snapshot without a generation changes nothing",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: ateletReported(t, 0)},
		want: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
	}, {
		name: "an upload URI other than the recorded one is refused",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: func() *ateletpb.SnapshotReport {
			r := ateletReported(t, 2)
			r.Snapshot.Object.SnapshotUri = ownedURI(t, reportActorUID, 9)
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "an unset state changes nothing",
		status: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		want: &ateapipb.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", pending),
		}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyCheckpointState(tt.status, tt.state)
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("applyCheckpointState() error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyCheckpointState() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, tt.status, protocmp.Transform()); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
