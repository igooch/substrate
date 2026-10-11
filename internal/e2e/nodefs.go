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

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/nodepath"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// nodeInspectorImage is the busybox the networkpolicy suite also runs, for the
// sh and ls the inspector execs.
const nodeInspectorImage = "busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e"

// nodeInspectorReadyTimeout covers a cold image pull on the node.
const nodeInspectorReadyTimeout = 3 * time.Minute

// nodeInspectorPollInterval spaces the listings a wait takes.
const nodeInspectorPollInterval = 2 * time.Second

// nodeInspectorExecTimeout bounds one kubectl exec into the inspector.
const nodeInspectorExecTimeout = 20 * time.Second

// NodeInspector is a pod pinned to one node that mounts atelet's per-actor
// directories read-only, so a test can check what atelet left on that node's
// disk. The API only records where a snapshot lives; a file atelet failed to
// delete is invisible to it.
type NodeInspector struct {
	Namespace string
	Pod       string
	Node      string
}

// NewNodeInspector starts an inspector pod on node in namespace and waits for
// it to run. It registers no cleanup: the pod goes with the namespace, and is
// retained with it when the suite fails.
func NewNodeInspector(t *testing.T, ctx context.Context, namespace, node string) *NodeInspector {
	t.Helper()
	created, err := GetClients().K8s.CoreV1().Pods(namespace).Create(ctx, nodeInspectorPod(namespace, node), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a node inspector pod on node %s: %v", node, err)
	}
	WaitForPodReady(t, ctx, namespace, created.Name, nodeInspectorReadyTimeout)
	return &NodeInspector{Namespace: namespace, Pod: created.Name, Node: node}
}

// nodeInspectorPod idles busybox on node with nodepath.ActorsDir mounted
// read-only at the same path, so a path inside the inspector is the path on
// the host.
func nodeInspectorPod(namespace, node string) *corev1.Pod {
	hostPathType := corev1.HostPathDirectory
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "node-inspector-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			// NodeName bypasses the scheduler, so the pod lands on the node
			// under test; tolerating every taint keeps it there.
			NodeName:    node,
			Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:    "inspector",
				Image:   nodeInspectorImage,
				Command: []string{"/bin/sleep", "3600"},
				// Root, because atelet creates the actor directories root-owned
				// and closed to everyone else.
				SecurityContext: &corev1.SecurityContext{RunAsUser: ptr.To[int64](0)},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "actors",
					MountPath: nodepath.ActorsDir,
					ReadOnly:  true,
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "actors",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: nodepath.ActorsDir, Type: &hostPathType},
				},
			}},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

// LocalCheckpoints lists the local snapshots atelet holds on the node for the
// actor: the sorted entries of its local-checkpoint directory. A missing
// directory is an empty list.
func (n *NodeInspector) LocalCheckpoints(actorUID string) ([]string, error) {
	entries, _, err := n.listDir(localCheckpointsDir(actorUID))
	return entries, err
}

// WaitForLocalCheckpoints polls LocalCheckpoints until it holds exactly want,
// in any order, and reports the last listing, or the last failure to list,
// as a test error if it does not within timeout.
func (n *NodeInspector) WaitForLocalCheckpoints(t *testing.T, actorUID string, want []string, timeout time.Duration) {
	t.Helper()
	want = slices.Sorted(slices.Values(want))
	var got []string
	var listErr error
	if !poll(timeout, func() bool {
		got, listErr = n.LocalCheckpoints(actorUID)
		return listErr == nil && slices.Equal(got, want)
	}) {
		if listErr != nil {
			t.Errorf("listing local checkpoints of actor %s on node %s: %v (waited %v)", actorUID, n.Node, listErr, timeout)
			return
		}
		t.Errorf("local checkpoints of actor %s on node %s = %q, want %q (waited %v)", actorUID, n.Node, got, want, timeout)
		return
	}
	t.Logf("local checkpoints of actor %s on node %s: %q", actorUID, n.Node, got)
}

// SnapshotState holds the snapshot UUIDs recorded in atelet's on-disk
// snapshot-state.json for an actor.
type SnapshotState struct {
	LatestLocalUUID   string
	LatestDurableUUID string
}

// SnapshotState reads atelet's snapshot-state.json for actorUID on the node.
func (n *NodeInspector) SnapshotState(actorUID string) (SnapshotState, error) {
	out, err := n.kubectlExec("cat", snapshotStateFile(actorUID))
	if err != nil {
		return SnapshotState{}, err
	}
	return parseSnapshotState(out)
}

// WaitForSnapshotState polls SnapshotState until it matches want, and reports
// the last state or read error as a test error if it does not within timeout.
func (n *NodeInspector) WaitForSnapshotState(t *testing.T, actorUID string, want SnapshotState, timeout time.Duration) {
	t.Helper()
	var got SnapshotState
	var readErr error
	if !poll(timeout, func() bool {
		got, readErr = n.SnapshotState(actorUID)
		return readErr == nil && got == want
	}) {
		if readErr != nil {
			t.Errorf("reading snapshot-state.json of actor %s on node %s: %v (waited %v)", actorUID, n.Node, readErr, timeout)
			return
		}
		t.Errorf("snapshot-state.json of actor %s on node %s = %+v, want %+v (waited %v)", actorUID, n.Node, got, want, timeout)
		return
	}
	t.Logf("snapshot-state.json of actor %s on node %s: %+v", actorUID, n.Node, got)
}

// WaitForActorDirRemoved polls until the actor's directory is gone from the
// node, which is what atelet's Terminate leaves behind, and reports what is
// still there, or the last failure to list it, as a test error if it is not
// within timeout.
func (n *NodeInspector) WaitForActorDirRemoved(t *testing.T, actorUID string, timeout time.Duration) {
	t.Helper()
	dir := actorDir(actorUID)
	var left []string
	var listErr error
	if !poll(timeout, func() bool {
		var exists bool
		left, exists, listErr = n.listDir(dir)
		return listErr == nil && !exists
	}) {
		if listErr != nil {
			t.Errorf("listing actor %s directory %s on node %s: %v (waited %v)", actorUID, dir, n.Node, listErr, timeout)
			return
		}
		t.Errorf("actor %s directory %s is still on node %s, holding %q (waited %v)", actorUID, dir, n.Node, left, timeout)
		return
	}
	t.Logf("actor %s directory is gone from node %s", actorUID, n.Node)
}

// listDir returns the sorted entries of dir on the node and whether it exists.
func (n *NodeInspector) listDir(dir string) (entries []string, exists bool, err error) {
	// The script prints the directory itself ahead of its entries, telling an
	// empty directory apart from a missing one. The path is an argument rather
	// than part of the script, so the shell interprets none of it.
	out, err := n.kubectlExec("sh", "-c", `if [ -d "$1" ]; then echo "$1"; ls -1A "$1"; fi`, "sh", dir)
	if err != nil {
		return nil, false, err
	}
	entries, exists, err = parseDirListing(out, dir)
	if err != nil {
		return nil, false, fmt.Errorf("listing %s on node %s: %w", dir, n.Node, err)
	}
	return entries, exists, nil
}

// kubectlExec runs argv in the inspector container and returns its stdout. A
// failed run reports kubectl's stderr.
func (n *NodeInspector) kubectlExec(argv ...string) ([]byte, error) {
	args := append([]string{"exec", "-n", n.Namespace, n.Pod, "--"}, argv...)
	if KubeContext != "" {
		args = append([]string{"--context=" + KubeContext}, args...)
	}
	if KubeConfig != "" {
		args = append([]string{"--kubeconfig=" + KubeConfig}, args...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeInspectorExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// poll calls done every nodeInspectorPollInterval until it reports true, and
// returns false if it has not by the time timeout passes.
func poll(timeout time.Duration, done func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(nodeInspectorPollInterval)
	}
	return true
}

// actorDir is the actor's directory on the host, where the inspector mounts it
// too. It and localCheckpointsDir mirror ateletpath.ActorPath and
// ateletpath.LocalCheckpointsDir in cmd/atelet/internal, which this package
// cannot import.
func actorDir(actorUID string) string {
	return filepath.Join(nodepath.ActorsDir, actorUID)
}

// localCheckpointsDir is the actor's local-checkpoint directory on the host.
func localCheckpointsDir(actorUID string) string {
	return filepath.Join(actorDir(actorUID), "local-checkpoint")
}

// snapshotStateFile is the actor's snapshot-state.json file on the host.
func snapshotStateFile(actorUID string) string {
	return filepath.Join(actorDir(actorUID), "snapshot-state.json")
}

func parseSnapshotState(raw []byte) (SnapshotState, error) {
	var rec struct {
		LatestLocal *struct {
			UUID string `json:"uuid"`
		} `json:"latestLocal"`
		LatestDurable *struct {
			UUID string `json:"uuid"`
		} `json:"latestDurable"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return SnapshotState{}, err
	}
	var st SnapshotState
	if rec.LatestLocal != nil {
		st.LatestLocalUUID = rec.LatestLocal.UUID
	}
	if rec.LatestDurable != nil {
		st.LatestDurableUUID = rec.LatestDurable.UUID
	}
	return st, nil
}

// parseDirListing splits listDir's output, which is empty for a missing
// directory and otherwise the directory followed by its entries, into the
// sorted entries and whether dir exists.
func parseDirListing(out []byte, dir string) (entries []string, exists bool, err error) {
	var lines []string
	for line := range strings.Lines(string(out)) {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		return nil, false, nil
	}
	if lines[0] != dir {
		return nil, false, fmt.Errorf("listing starts with %q, want the directory %q", lines[0], dir)
	}
	entries = lines[1:]
	slices.Sort(entries)
	return entries, true, nil
}
