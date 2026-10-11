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
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// restoreLine is an atelet log line for a restore of actorUID from a kind
// snapshot at the given time, with any extra fields.
func restoreLine(t *testing.T, msg string, at time.Time, actorUID, kind string, extra map[string]string) string {
	t.Helper()
	fields := map[string]string{
		slog.TimeKey:                    at.Format(time.RFC3339Nano),
		slog.MessageKey:                 msg,
		string(ateattr.ActorUIDKey):     actorUID,
		string(ateattr.SnapshotKindKey): kind,
	}
	for k, v := range extra {
		fields[k] = v
	}
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRestoresIn(t *testing.T) {
	since := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return since.Add(d) }
	logs := strings.Join([]string{
		restoreLine(t, restoreLogMessage, at(-time.Millisecond), "a1", ateattr.SnapshotKindGolden, nil),
		restoreLine(t, "Handle RPC", at(time.Second), "a1", ateattr.SnapshotKindLatest, nil),
		"panic: not a JSON line",
		restoreLine(t, restoreLogMessage, at(2*time.Second), "a2", ateattr.SnapshotKindLatest, nil),
		restoreLine(t, restoreLogMessage, at(3*time.Second), "a1", ateattr.SnapshotKindLatest,
			map[string]string{string(ateattr.ErrorTypeKey): "Internal"}),
		restoreLine(t, restoreLogMessage, at(4*time.Second), "a1", ateattr.SnapshotKindLocal, nil),
		restoreLine(t, restoreLogMessage, since, "a1", ateattr.SnapshotKindLatest, nil),
	}, "\n")

	got, err := restoresIn([]byte(logs), "a1", since)
	if err != nil {
		t.Fatalf("restoresIn: %v", err)
	}
	// Log order: the restore at exactly since was logged last.
	want := []actorRestore{
		{time: at(4 * time.Second), kind: ateattr.SnapshotKindLocal},
		{time: since, kind: ateattr.SnapshotKindLatest},
	}
	if len(got) != len(want) {
		t.Fatalf("restoresIn = %+v, want %+v", got, want)
	}
	for i := range want {
		if !got[i].time.Equal(want[i].time) || got[i].kind != want[i].kind {
			t.Errorf("restoresIn[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// restoresIn reads what slog's JSON handler, atelet's, writes.
func TestRestoresInReadsSlogJSON(t *testing.T) {
	at := time.Date(2026, 10, 11, 3, 0, 0, 123456789, time.UTC)
	var buf bytes.Buffer
	pinTime := func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Time(slog.TimeKey, at)
		}
		return a
	}
	slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{ReplaceAttr: pinTime})).LogAttrs(context.Background(), slog.LevelInfo, restoreLogMessage,
		slog.String(string(ateattr.ActorUIDKey), "a1"),
		slog.String(string(ateattr.SnapshotKindKey), ateattr.SnapshotKindLocal))

	got, err := restoresIn(buf.Bytes(), "a1", at)
	if err != nil {
		t.Fatalf("restoresIn: %v", err)
	}
	if len(got) != 1 || !got[0].time.Equal(at) || got[0].kind != ateattr.SnapshotKindLocal {
		t.Errorf("restoresIn(%s) = %+v, want one local restore at %s", buf.Bytes(), got, at)
	}
}

func TestRestoresInRejectsMalformedRestoreLine(t *testing.T) {
	since := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)
	for name, line := range map[string]string{
		"not JSON": restoreLogMessage + " for actor a1",
		"bad time": strings.Replace(restoreLine(t, restoreLogMessage, since, "a1", ateattr.SnapshotKindLocal, nil),
			since.Format(time.RFC3339Nano), "yesterday", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := restoresIn([]byte(line), "a1", since); err == nil {
				t.Errorf("restoresIn(%q) = %+v, want an error", line, got)
			}
		})
	}
}
