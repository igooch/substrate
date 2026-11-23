// Copyright 2026 The Agent Substrate Authors
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

package main

import "testing"

func TestLocalSnapshotDeficit(t *testing.T) {
	tests := []struct {
		name        string
		total, used int64
		want        int64
	}{
		{name: "under the high mark", total: 1000, used: 800},
		{name: "over the high mark frees down to the low mark", total: 1000, used: 820, want: 70},
		{name: "full", total: 1000, used: 1000, want: 250},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := localSnapshotDeficit(tc.total, tc.used, 80, 75); got != tc.want {
				t.Errorf("localSnapshotDeficit(%d, %d) = %d, want %d", tc.total, tc.used, got, tc.want)
			}
		})
	}
}
