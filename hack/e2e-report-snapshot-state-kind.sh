#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Walks one actor through an asynchronous suspend on a kind cluster and checks
# that atelet's ReportActorSnapshotState reports reach the control plane:
#
#   1. pause: the snapshot is local only, durable copy PENDING
#   2. upload delay: the report marks the durable copy COMPLETED, still local
#   3. detach delay: the report clears locality and assigned_node
#   4. resume: the actor runs again, restored from object storage
#
# Prerequisites: a kind cluster installed with
#   hack/install-ate-kind.sh --deploy-ate-system --deploy-demo-counter
# from this branch, plus jq. It shortens atelet's timers on the cluster.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

CONTEXT="${CONTEXT:-kind-kind}"
UPLOAD_DELAY="${UPLOAD_DELAY:-20s}"
DETACH_DELAY="${DETACH_DELAY:-60s}"
ATESPACE="${ATESPACE:-ate-demo-counter}"
TEMPLATE="${TEMPLATE:-counter}"
ACTOR="${ACTOR:-report-e2e-$(date +%s)}"
TIMEOUT="${TIMEOUT:-180}"

K=(kubectl --context "${CONTEXT}")
go build -o bin/kubectl-ate ./cmd/kubectl-ate
KA=(bin/kubectl-ate --context "${CONTEXT}")

log() { echo "--- $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

actor_json() { "${KA[@]}" get actor "${ACTOR}" -a "${ATESPACE}" -o json; }

# wait_for describes a condition, then polls the jq filter on the actor until
# it prints true.
wait_for() {
  local what="$1" filter="$2" deadline=$((SECONDS + TIMEOUT))
  log "waiting until ${what}"
  until [[ "$(actor_json | jq -r "${filter}")" == "true" ]]; do
    if ((SECONDS > deadline)); then
      actor_json | jq '.status' >&2
      fail "timed out waiting until ${what}"
    fi
    sleep 2
  done
}

log "setting atelet --snapshot-upload-delay=${UPLOAD_DELAY} --actor-detach-delay=${DETACH_DELAY}"
for ds in $("${K[@]}" -n ate-system get ds -o name | grep '/atelet'); do
  idx=$("${K[@]}" -n ate-system get "${ds}" -o json \
    | jq '.spec.template.spec.containers | map(.name) | index("atelet")')
  args=$("${K[@]}" -n ate-system get "${ds}" -o json \
    | jq -c --arg u "--snapshot-upload-delay=${UPLOAD_DELAY}" --arg d "--actor-detach-delay=${DETACH_DELAY}" \
      ".spec.template.spec.containers[${idx}].args
       | map(select(startswith(\"--snapshot-upload-delay=\") or startswith(\"--actor-detach-delay=\") | not))
       + [\$u, \$d]")
  "${K[@]}" -n ate-system patch "${ds}" --type json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/${idx}/args\",\"value\":${args}}]"
  "${K[@]}" -n ate-system rollout status "${ds}" --timeout=300s
done

log "creating actor ${ATESPACE}/${ACTOR}"
"${KA[@]}" create actor "${ACTOR}" -a "${ATESPACE}" --template "${TEMPLATE}"
# A new actor starts SUSPENDED.
"${KA[@]}" resume actor "${ACTOR}" -a "${ATESPACE}"
wait_for "the actor is RUNNING" '.status.state == "ACTOR_STATE_RUNNING"'

log "pausing"
"${KA[@]}" pause actor "${ACTOR}" -a "${ATESPACE}"
wait_for "the actor is PAUSED" '.status.state == "ACTOR_STATE_PAUSED"'
NODE=$(actor_json | jq -r '.status.assignedNode')
[[ -n "${NODE}" && "${NODE}" != "null" ]] || fail "a paused actor has no assigned node"
log "paused on node ${NODE}"
actor_json | jq '.status.snapshots'

wait_for "the upload is reported: durable copy COMPLETED, still local" \
  "[.status.snapshots[] | select(.locality == \"${NODE}\") | .durableSnapshot.status] | index(\"SNAPSHOT_STORAGE_STATUS_COMPLETED\") != null"
actor_json | jq '.status.snapshots'

wait_for "the detach is reported: no locality, no assigned node" \
  '(.status.assignedNode // "") == "" and ([.status.snapshots[] | select((.locality // "") != "")] | length) == 0'
actor_json | jq '{assignedNode: .status.assignedNode, snapshots: .status.snapshots}'

log "atelet's report log lines on ${NODE}"
POD=$("${K[@]}" -n ate-system get pods -o wide --field-selector "spec.nodeName=${NODE}" | awk '/atelet/ {print $1; exit}')
"${K[@]}" -n ate-system logs "${POD}" -c atelet | grep -E 'actor snapshot state|Detached actor' | grep -F "$(actor_json | jq -r '.metadata.uid')" || true

log "resuming"
"${KA[@]}" resume actor "${ACTOR}" -a "${ATESPACE}"
wait_for "the actor is RUNNING again" '.status.state == "ACTOR_STATE_RUNNING"'

log "PASS: ${ATESPACE}/${ACTOR} was uploaded, detached, and resumed from object storage"
