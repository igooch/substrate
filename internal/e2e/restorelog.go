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
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
)

// restoreLogMessage is the message of the line atelet logs as each Restore
// returns. The line names the actor and, in ateattr.SnapshotKindKey, where
// the snapshot came from.
const restoreLogMessage = "Restore timing breakdown"

// ateletPodSelector and ateletContainer locate atelet's log in the system
// namespace.
const (
	ateletPodSelector = "app=atelet"
	ateletContainer   = "atelet"
)

// restoreLogReadTimeout bounds one read of an atelet's log.
const restoreLogReadTimeout = 10 * time.Second

// actorRestore is one successful restore of an actor as atelet logged it.
type actorRestore struct {
	time time.Time
	kind string
}

// WaitForActorRestore polls the log of the atelet on node until it records a
// successful restore of the actor at or after since, and reports a test error
// unless the newest such restore has snapshot kind want:
// ateattr.SnapshotKindLocal for the node's own copy, SnapshotKindLatest for
// the actor's snapshot in object storage, SnapshotKindGolden for the
// template's golden snapshot. The API does not record where a restore came
// from. Pass the node the actor resumed on and the time just before
// ResumeActor to check that resume. Failed reads are retried until timeout.
func WaitForActorRestore(t *testing.T, ctx context.Context, node, actorUID string, since time.Time, want string, timeout time.Duration) {
	t.Helper()
	var restores []actorRestore
	var readErr error
	if !poll(timeout, func() bool {
		restores, readErr = actorRestores(ctx, node, actorUID, since)
		return readErr == nil && len(restores) > 0
	}) {
		if readErr != nil {
			t.Errorf("reading the restores of actor %s on node %s: %v (waited %v)", actorUID, node, readErr, timeout)
			return
		}
		t.Errorf("the atelet on node %s logged no restore of actor %s since %s (waited %v)", node, actorUID, since.Format(time.RFC3339Nano), timeout)
		return
	}
	newest := restores[len(restores)-1]
	if newest.kind != want {
		t.Errorf("actor %s restored from a %q snapshot on node %s, want %q", actorUID, newest.kind, node, want)
		return
	}
	t.Logf("actor %s restored from a %q snapshot on node %s", actorUID, newest.kind, node)
}

// actorRestores returns the successful restores of the actor that the
// running atelet on node logged at or after since, oldest first.
func actorRestores(ctx context.Context, node, actorUID string, since time.Time) ([]actorRestore, error) {
	pods := GetClients().K8s.CoreV1().Pods(SystemNamespace())
	list, err := pods.List(ctx, metav1.ListOptions{
		LabelSelector: ateletPodSelector,
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("listing atelet pods: %w", err)
	}
	// SinceTime only trims what is read: it is sent with whole-second
	// precision, and restoresIn applies since exactly.
	sinceTime := metav1.NewTime(since)
	var all []actorRestore
	read := 0
	for _, pod := range list.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		read++
		readCtx, cancel := context.WithTimeout(ctx, restoreLogReadTimeout)
		logs, err := pods.GetLogs(pod.Name, &corev1.PodLogOptions{Container: ateletContainer, SinceTime: &sinceTime}).DoRaw(readCtx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("reading the log of atelet pod %s: %w", pod.Name, err)
		}
		restores, err := restoresIn(logs, actorUID, since)
		if err != nil {
			return nil, fmt.Errorf("reading the log of atelet pod %s: %w", pod.Name, err)
		}
		all = append(all, restores...)
	}
	if read == 0 {
		return nil, fmt.Errorf("no running atelet pod on node %s", node)
	}
	slices.SortStableFunc(all, func(a, b actorRestore) int { return a.time.Compare(b.time) })
	return all, nil
}

// restoresIn returns the successful restores of actorUID that the atelet log
// lines in logs record at or after since, in log order. A failed restore
// carries ateattr.ErrorTypeKey and is left out.
func restoresIn(logs []byte, actorUID string, since time.Time) ([]actorRestore, error) {
	var restores []actorRestore
	for line := range bytes.Lines(logs) {
		if !bytes.Contains(line, []byte(restoreLogMessage)) {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			return nil, fmt.Errorf("decoding restore log line %q: %w", line, err)
		}
		if fields[slog.MessageKey] != restoreLogMessage || fields[string(ateattr.ActorUIDKey)] != actorUID {
			continue
		}
		if _, failed := fields[string(ateattr.ErrorTypeKey)]; failed {
			continue
		}
		stamp, _ := fields[slog.TimeKey].(string)
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			return nil, fmt.Errorf("restore log line %q: %w", line, err)
		}
		if at.Before(since) {
			continue
		}
		kind, _ := fields[string(ateattr.SnapshotKindKey)].(string)
		restores = append(restores, actorRestore{time: at, kind: kind})
	}
	return restores, nil
}
