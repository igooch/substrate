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
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	reportAtespace = "team-a"
	reportActorUID = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"
	reportNode     = "node-a"
	reportOther    = "node-b"
)

func snapName(gen int32) string { return fmt.Sprintf("snap-%d", gen) }

// ownedURI returns the upload URI of actorUID's snapshot of generation gen.
func ownedURI(t *testing.T, actorUID string, gen int32) string {
	t.Helper()
	uri, err := resources.NewActorSnapshotURI("gs://snapshots", reportAtespace, actorUID, snapName(gen))
	if err != nil {
		t.Fatal(err)
	}
	return uri.String()
}

// localOnly returns a Snapshot held on locality with no durable copy.
func localOnly(gen int32, locality string) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{Uuid: snapName(gen), Generation: gen, Locality: locality, Fidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}
}

// withDurable returns a Snapshot with a durable copy in the given storage
// status.
func withDurable(t *testing.T, gen int32, locality string, storage ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	t.Helper()
	snap := localOnly(gen, locality)
	snap.DurableSnapshot = &ateapipb.SnapshotStorage{
		Status: storage,
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: ownedURI(t, reportActorUID, gen)},
	}
	return snap
}

func reported(t *testing.T, gen int32) *ateapipb.SnapshotReport {
	t.Helper()
	return reportedAt(t, gen, ownedURI(t, reportActorUID, gen))
}

func reportedAt(t *testing.T, gen int32, uri string) *ateapipb.SnapshotReport {
	t.Helper()
	return &ateapipb.SnapshotReport{
		Uuid:       snapName(gen),
		Generation: gen,
		Fidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:     &ateapipb.ObjectSnapshot{SnapshotUri: uri},
	}
}

func TestApplyActorSnapshotState(t *testing.T) {
	const (
		pending   = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		completed = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		suspended = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		running   = ateapipb.ActorState_ACTOR_STATE_RUNNING
	)
	elsewhere, err := resources.NewActorSnapshotURI("gs://elsewhere", reportAtespace, reportActorUID, snapName(2))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		status   *ateapipb.ActorStatus
		state    *ateapipb.ActorSnapshotState
		want     *ateapipb.ActorStatus
		wantCode codes.Code
	}{{
		name: "upload of a snapshot with no recorded upload location is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "upload completes a recorded pending durable copy",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "upload prunes the older durable snapshot",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, "", completed),
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "an older snapshot still uploading keeps its entry",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), Uploading: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportNode),
		}},
	}, {
		name: "the older upload's completion is recorded after the newer snapshot",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
			localOnly(3, reportNode),
		}},
	}, {
		name: "an upload in flight with no local snapshot is not a detach",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{Uploading: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
	}, {
		name: "a report from one node leaves another node's newer snapshot alone",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportOther),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
			localOnly(3, reportOther),
		}},
	}, {
		name: "duplicate report changes nothing",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "stale durable report is a no-op",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 3, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 3, reportNode, completed),
		}},
	}, {
		name: "upload of a generation the record has moved past is a no-op",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportNode),
		}},
	}, {
		name: "upload of a snapshot the record has dropped is allowed",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), Uploading: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportNode),
		}},
	}, {
		name: "upload of a generation a revert discarded is a no-op",
		status: &ateapipb.ActorStatus{State: suspended, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 3)},
		want: &ateapipb.ActorStatus{State: suspended, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "upload records the fidelity the manifest reports",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapipb.SnapshotReport {
			r := reported(t, 2)
			r.Fidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			return r
		}()},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			func() *ateapipb.Snapshot {
				snap := withDurable(t, 2, reportNode, completed)
				snap.Fidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
				return snap
			}(),
		}},
	}, {
		name: "another node's snapshot cannot be reported durable",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportOther),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 3)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "an upload recorded on no node cannot be completed",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			withDurable(t, 3, "", pending),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 3)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "an upload atelet gave up on is dropped",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportNode),
		}},
	}, {
		name: "an upload of a recorded snapshot with no upload location is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportNode),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), Uploading: reported(t, 2)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "another node's snapshot cannot be reported as local",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportOther),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "another node's snapshot cannot be reported as uploading",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportOther),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), Uploading: reported(t, 3)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "a duplicate with the wrong uri is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reportedAt(t, 2, elsewhere.String())},
		wantCode: codes.InvalidArgument,
	}, {
		name: "a duplicate with the wrong uuid is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: func() *ateapipb.SnapshotReport {
			r := reported(t, 2)
			r.Uuid = "other"
			return r
		}()},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "unknown durable generation is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 5)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "unknown local generation is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 7)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "uploading uuid mismatch is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), Uploading: func() *ateapipb.SnapshotReport {
			r := reported(t, 2)
			r.Uuid = "other"
			return r
		}()},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "an upload to a URI other than the recorded one is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
			localOnly(3, reportNode),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), Uploading: reportedAt(t, 2, ownedURI(t, reportActorUID, 9))},
		wantCode: codes.InvalidArgument,
	}, {
		name: "a local snapshot naming a URI other than the recorded one is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reportedAt(t, 2, ownedURI(t, reportActorUID, 9))},
		wantCode: codes.InvalidArgument,
	}, {
		name: "durable uuid mismatch is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapipb.SnapshotReport {
			r := reported(t, 2)
			r.Uuid = "other"
			return r
		}()},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "uri other than the recorded one is refused",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state:    &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reportedAt(t, 2, ownedURI(t, reportActorUID, 9))},
		wantCode: codes.InvalidArgument,
	}, {
		name: "detach releases a suspended actor from the node",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a detach that deleted the only copy of the latest snapshot crashes the actor",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, "", completed),
			localOnly(2, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 1)},
		want: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_CRASHED,
			LastAssignedGeneration: 2,
			Crash:                  &ateapipb.ActorCrash{Message: "suspend failed: atelet on node node-a deleted snapshot generation 2 before it was durable"},
			Snapshots: []*ateapipb.Snapshot{
				withDurable(t, 1, "", completed),
			},
		},
	}, {
		name: "a detach that leaves only a pending upload no node holds crashes the actor",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			withDurable(t, 3, "", pending),
		}},
		state: &ateapipb.ActorSnapshotState{},
		want: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_CRASHED,
			LastAssignedGeneration: 3,
			Crash:                  &ateapipb.ActorCrash{Message: "suspend failed: atelet on node node-a deleted snapshot generation 2 before it was durable"},
		},
	}, {
		name: "a detach of an older snapshot that never became durable only drops it",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			localOnly(3, reportOther),
		}},
		state: &ateapipb.ActorSnapshotState{},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(3, reportOther),
		}},
	}, {
		name: "detach releases a paused actor from the node",
		status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a running actor's detach drops the copy it no longer holds",
		status: &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{},
		want:  &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, LastAssignedGeneration: 2},
	}, {
		name: "detach keeps the assigned node of a running actor",
		status: &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "detach from the old node leaves the new node alone",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, reportNode, completed),
			localOnly(3, reportOther),
		}},
		state: &ateapipb.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 2, "", completed),
			localOnly(3, reportOther),
		}},
	}, {
		name: "older local snapshot atelet no longer lists is dropped",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, "", completed),
			localOnly(2, reportNode),
			localOnly(3, reportNode),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 3), LatestDurable: reported(t, 1)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapipb.Snapshot{
			withDurable(t, 1, "", completed),
			localOnly(3, reportNode),
		}},
	}, {
		name: "a pending upload no node holds is dropped even at the newest generation",
		status: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
			withDurable(t, 3, "", pending),
		}},
		state: &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2)},
		want: &ateapipb.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{
			localOnly(2, reportNode),
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
			if diff := cmp.Diff(tt.want, tt.status, protocmp.Transform(), protocmp.IgnoreFields(&ateapipb.ActorCrash{}, "crash_time")); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Upload URIs that the actor no longer records are released, except the one
// atelet is still uploading.
func TestReplacedDurableURIs(t *testing.T) {
	const (
		pending   = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		completed = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	)
	actor := func(snaps ...*ateapipb.Snapshot) *ateapipb.Actor {
		return &ateapipb.Actor{Status: &ateapipb.ActorStatus{Snapshots: snaps}}
	}
	before := actor(withDurable(t, 1, "", completed), withDurable(t, 2, "", completed), withDurable(t, 3, "", pending), withDurable(t, 4, reportNode, completed))
	after := actor(withDurable(t, 4, reportNode, completed))
	state := &ateapipb.ActorSnapshotState{Uploading: reported(t, 2)}

	got := replacedDurableURIs(before, after, state)
	want := []string{ownedURI(t, reportActorUID, 1), ownedURI(t, reportActorUID, 3)}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("replacedDurableURIs() mismatch (-want +got):\n%s", diff)
	}
}

// A report is applied to the stored actor, and the snapshot it replaces is
// released. A refused report writes and releases nothing.
func TestApplyReportedSnapshotState(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	objects := objectstoretest.New()
	w := NewActorWorkflow(persistence, nil, nil, nil, nil, nil, "", nil, objectstoreplugintest.ControlClient(objects))
	tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")

	actorRef := resources.ActorRef{Atespace: reportAtespace, Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: reportAtespace, Name: "tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	uid := actor.GetMetadata().GetUid()
	old := mustActorSnapshotURI(t, tmpl, actor, "snap-1")
	objects.PutSnapshot(t, old, "manifest.json")
	pending := mustActorSnapshotURI(t, tmpl, actor, "snap-2")
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.AssignedNode = reportNode
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", old.Name(), old.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			func() *ateapipb.Snapshot {
				snap := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", pending.Name(), pending.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
				snap.Locality = reportNode
				return snap
			}(),
		}
	})
	uploaded := reportedAt(t, 2, pending.String())
	state := &ateapipb.ActorSnapshotState{LatestLocal: uploaded, LatestDurable: uploaded}

	t.Run("refused while another operation holds the actor", func(t *testing.T) {
		lease, err := persistence.AcquireLease(ctx, actorLeaseKey(actorRef))
		if err != nil {
			t.Fatalf("AcquireLease() failed: %v", err)
		}
		defer lease.Close()
		if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); apierror.Code(err) != codes.Aborted {
			t.Fatalf("ApplyReportedSnapshotState() = %v, want Aborted", err)
		}
	})
	for _, tc := range []struct {
		name, uid, node string
		ref             resources.ActorRef
	}{
		{"refused for another node", uid, reportOther, actorRef},
		{"refused for a stale incarnation", "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17", reportNode, actorRef},
		{"refused for a missing actor", uid, reportNode, resources.ActorRef{Atespace: reportAtespace, Name: "no-such-actor"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := w.ApplyReportedSnapshotState(ctx, tc.ref, tc.uid, tc.node, state); apierror.Code(err) != codes.NotFound {
				t.Fatalf("ApplyReportedSnapshotState() = %v, want NotFound", err)
			}
		})
	}
	// A transitioning actor gets Aborted, and a crashed one FailedPrecondition.
	for _, tc := range []struct {
		state ateapipb.ActorState
		want  codes.Code
	}{
		{ateapipb.ActorState_ACTOR_STATE_SUSPENDING, codes.Aborted},
		{ateapipb.ActorState_ACTOR_STATE_PAUSING, codes.Aborted},
		{ateapipb.ActorState_ACTOR_STATE_RESUMING, codes.Aborted},
		{ateapipb.ActorState_ACTOR_STATE_DELETING, codes.Aborted},
		{ateapipb.ActorState_ACTOR_STATE_REVERTING, codes.Aborted},
		{ateapipb.ActorState_ACTOR_STATE_CRASHED, codes.FailedPrecondition},
	} {
		t.Run("refused for a "+tc.state.String()+" actor", func(t *testing.T) {
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) { s.State = tc.state })
			defer func() {
				actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
					s.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
				})
			}()
			if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); apierror.Code(err) != tc.want {
				t.Fatalf("ApplyReportedSnapshotState() = %v, want %v", err, tc.want)
			}
			if got, err := persistence.GetActor(ctx, actorRef); err != nil {
				t.Fatal(err)
			} else if got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
				t.Errorf("a refused report wrote the actor: version %d, want %d", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
			}
		})
	}
	t.Run("an upload to the wrong URI is refused and changes nothing", func(t *testing.T) {
		wrong := &ateapipb.ActorSnapshotState{LatestLocal: uploaded, LatestDurable: reportedAt(t, 2, old.String())}
		if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, wrong); apierror.Code(err) != codes.InvalidArgument {
			t.Fatalf("ApplyReportedSnapshotState() = %v, want InvalidArgument", err)
		}
		if left := objects.Prefix(t, old.Prefix()); len(left) == 0 {
			t.Error("a refused report released a snapshot")
		}
	})
	got, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
		t.Fatalf("a refused report wrote the actor: version %d, want %d", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
	}
	if diff := cmp.Diff(actor.GetStatus(), got.GetStatus(), protocmp.Transform()); diff != "" {
		t.Fatalf("a refused report changed the actor (-want +got):\n%s", diff)
	}

	if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); err != nil {
		t.Fatalf("ApplyReportedSnapshotState() failed: %v", err)
	}
	got, err = persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatal(err)
	}
	want := &ateapipb.Snapshot{
		Uuid:       "snap-2",
		Generation: 2,
		Locality:   reportNode,
		Fidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Owner:      ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: pending.String()},
		},
	}
	if diff := cmp.Diff([]*ateapipb.Snapshot{want}, got.GetStatus().GetSnapshots(), protocmp.Transform()); diff != "" {
		t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
	}
	if left := objects.Prefix(t, old.Prefix()); len(left) != 0 {
		t.Errorf("replaced durable snapshot still holds %v, want it released", left)
	}

	// A report that changes nothing writes nothing.
	if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); err != nil {
		t.Fatalf("repeated ApplyReportedSnapshotState() failed: %v", err)
	}
	again, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetMetadata().GetVersion() != got.GetMetadata().GetVersion() {
		t.Errorf("an unchanged report wrote the actor: version %d, want %d", again.GetMetadata().GetVersion(), got.GetMetadata().GetVersion())
	}
}

// A detach that deleted the only copy of a suspended actor's latest snapshot
// crashes the actor and releases it from the node. An upload atelet gave up
// on is pruned, and its objects are released.
func TestApplyReportedSnapshotState_CrashAndRelease(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	objects := objectstoretest.New()
	w := NewActorWorkflow(persistence, nil, nil, nil, nil, nil, "", nil, objectstoreplugintest.ControlClient(objects))
	tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
	seed := func(name string, snaps func(actor *ateapipb.Actor) []*ateapipb.Snapshot) (resources.ActorRef, *ateapipb.Actor) {
		ref := resources.ActorRef{Atespace: reportAtespace, Name: name}
		actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: reportAtespace, Name: "tmpl"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		return ref, mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
			s.AssignedNode = reportNode
			s.LastAssignedGeneration = 2
			s.Snapshots = snaps(actor)
		})
	}

	t.Run("detach of the only copy crashes the actor", func(t *testing.T) {
		ref, actor := seed("crashes", func(*ateapipb.Actor) []*ateapipb.Snapshot {
			return []*ateapipb.Snapshot{localOnly(2, reportNode)}
		})
		if err := w.ApplyReportedSnapshotState(ctx, ref, actor.GetMetadata().GetUid(), reportNode, &ateapipb.ActorSnapshotState{}); err != nil {
			t.Fatalf("ApplyReportedSnapshotState() failed: %v", err)
		}
		got, err := persistence.GetActor(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED || got.GetStatus().GetCrash().GetMessage() == "" {
			t.Errorf("state = %v, crash = %v; want CRASHED with a crash message", got.GetStatus().GetState(), got.GetStatus().GetCrash())
		}
		if node := got.GetStatus().GetAssignedNode(); node != "" {
			t.Errorf("assigned_node = %q, want it cleared: the node holds nothing for the actor", node)
		}
	})

	t.Run("an abandoned upload is released", func(t *testing.T) {
		var abandoned resources.SnapshotURI
		ref, actor := seed("releases", func(actor *ateapipb.Actor) []*ateapipb.Snapshot {
			abandoned = mustActorSnapshotURI(t, tmpl, actor, "snap-1")
			objects.PutSnapshot(t, abandoned, "memory.zst")
			snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", abandoned.Name(), abandoned.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
			snap.Locality = reportNode
			return []*ateapipb.Snapshot{snap, localOnly(2, reportNode)}
		})
		if err := w.ApplyReportedSnapshotState(ctx, ref, actor.GetMetadata().GetUid(), reportNode, &ateapipb.ActorSnapshotState{LatestLocal: reported(t, 2)}); err != nil {
			t.Fatalf("ApplyReportedSnapshotState() failed: %v", err)
		}
		got, err := persistence.GetActor(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]*ateapipb.Snapshot{localOnly(2, reportNode)}, got.GetStatus().GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
		}
		if left := objects.Prefix(t, abandoned.Prefix()); len(left) != 0 {
			t.Errorf("abandoned upload still holds %v, want it released", left)
		}
	})
}
