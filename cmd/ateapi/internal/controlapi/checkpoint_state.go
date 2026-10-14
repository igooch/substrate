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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// applyCheckpointState records an upload that atelet's state, as returned by
// Checkpoint or PromoteSnapshot, says finished, with the same rules as a
// report. Unlike a report, a state with no local snapshot is not a detach: the
// workflow that made the call records where its snapshot lives.
func applyCheckpointState(status *ateapipb.ActorStatus, state *ateletpb.ActorSnapshotState) error {
	return applyLatestDurable(status, snapshotReportFromAtelet(state.GetLatestDurable()))
}
