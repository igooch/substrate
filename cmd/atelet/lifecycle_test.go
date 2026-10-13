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

package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/trustbundle"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/clock"
)

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origStatic := nodepath.ActorsDir, nodepath.StaticFilesDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.StaticFilesDir = filepath.Join(root, "static-files")
	t.Cleanup(func() {
		nodepath.ActorsDir, nodepath.StaticFilesDir = origActors, origStatic
	})
}

// fakeAteom is a fake ateom in a worker pod. It writes the files a
// real checkpoint would leave in the checkpoint dir, and reads back what a
// restore was handed. Like a real ateom it takes every actor directory from
// the request, never derived from the actor UID.
type fakeAteom struct {
	ateompb.UnimplementedAteomServer
	// snapshotFiles are written at checkpoint and reported back to atelet as
	// the exact set the snapshot consists of.
	snapshotFiles map[string]string
	// restored holds the file contents staged into the restore dir by the
	// most recent RestoreWorkload.
	restored map[string]string
	// actorDirs records the ActorDirs each RPC arrived with, by RPC name.
	actorDirs map[string]*ateompb.ActorDirs
	// preserveRestoreDir records the PreserveRestoreDir flag from the most
	// recent RestoreWorkload request.
	preserveRestoreDir bool
	// fidelities records the fidelity each checkpoint or restore arrived
	// with, by RPC name.
	fidelities map[string]ateompb.SnapshotFidelity
}

func (f *fakeAteom) recordFidelity(rpc string, fidelity ateompb.SnapshotFidelity) {
	if f.fidelities == nil {
		f.fidelities = map[string]ateompb.SnapshotFidelity{}
	}
	f.fidelities[rpc] = fidelity
}

func (f *fakeAteom) recordActorDirs(rpc string, actorDirs *ateompb.ActorDirs) {
	if f.actorDirs == nil {
		f.actorDirs = map[string]*ateompb.ActorDirs{}
	}
	f.actorDirs[rpc] = actorDirs
}

func (f *fakeAteom) RunWorkload(_ context.Context, req *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	f.recordActorDirs("RunWorkload", req.GetActorDirs())
	return &ateompb.RunWorkloadResponse{}, nil
}

func (f *fakeAteom) CheckpointWorkload(_ context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	f.recordActorDirs("CheckpointWorkload", req.GetActorDirs())
	f.recordFidelity("CheckpointWorkload", req.GetFidelity())
	dir := req.GetActorDirs().GetCheckpointDir()
	names := make([]string, 0, len(f.snapshotFiles))
	for name, body := range f.snapshotFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: names}, nil
}

func (f *fakeAteom) RestoreWorkload(_ context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	f.recordActorDirs("RestoreWorkload", req.GetActorDirs())
	f.recordFidelity("RestoreWorkload", req.GetFidelity())
	f.preserveRestoreDir = req.GetPreserveRestoreDir()
	dir := req.GetActorDirs().GetRestoreDir()
	f.restored = map[string]string{}
	for name := range f.snapshotFiles {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		f.restored[name] = string(body)
	}
	return &ateompb.RestoreWorkloadResponse{}, nil
}

func (f *fakeAteom) TerminateWorkload(_ context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	f.recordActorDirs("TerminateWorkload", req.GetActorDirs())
	return &ateompb.TerminateWorkloadResponse{}, nil
}

// serveFakeAteom serves ateom on a unix socket and points atelet's dialer at
// it. The socket lives in its own short temp dir.
func serveFakeAteom(t *testing.T, f *fakeAteom) {
	t.Helper()
	dir, err := os.MkdirTemp("", "ateom-")
	if err != nil {
		t.Fatalf("creating socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	sock := filepath.Join(dir, "ateom.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %q: %v", sock, err)
	}
	srv := grpc.NewServer()
	ateompb.RegisterAteomServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	orig := ateomSocketPath
	ateomSocketPath = func(string) string { return sock }
	t.Cleanup(func() { ateomSocketPath = orig })
}

// lifecycleSnapshot is the Snapshot the control plane names for an actor's
// checkpoint at generation.
func lifecycleSnapshot(actorUID, name string, generation int32, fidelity ateletpb.SnapshotFidelity) *ateletpb.Snapshot {
	return &ateletpb.Snapshot{
		SnapshotUuid: name,
		Fidelity:     fidelity,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://test-bucket/atespaces/ate-demo/actors/" + actorUID + "/snapshots/" + name},
		Generation:   generation,
	}
}

// useSnapshotTracker gives s a snapshot tracker that uploads uploadDelay after
// a deferred checkpoint and detaches detachDelay after one. Its workers stop
// once the test ends, before the node directories are restored.
func useSnapshotTracker(t *testing.T, s *AteomHerder, uploadDelay, detachDelay time.Duration) *snapshotTracker {
	t.Helper()
	tracker := newSnapshotTracker(t.Context(), s, clock.RealClock{})
	tracker.uploadDelay, tracker.detachDelay = uploadDelay, detachDelay
	tracker.start()
	t.Cleanup(tracker.wait)
	s.snapshots = tracker
	return tracker
}

// TestLocalSnapshotGC walks an actor through
// run -> pause -> resume -> pause -> terminate over atelet's RPC surface and
// ensures that superseded local snapshots are pruned by the next checkpoint
// and all remaining state is garbage collected after the actor is terminated.
// It runs at every fidelity atelet forwards, pinning that the pause and the
// resume hand ateom the fidelity the control plane asked for, unchanged.
func TestLocalSnapshotGC(t *testing.T) {
	for _, fidelity := range []ateletpb.SnapshotFidelity{
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	} {
		t.Run(fidelity.String(), func(t *testing.T) { runLocalSnapshotGC(t, fidelity) })
	}
}

func runLocalSnapshotGC(t *testing.T, fidelity ateletpb.SnapshotFidelity) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace      = "ate-demo"
		actorName     = "counter"
		actorUID      = "actor-uid-1"
		workerPodUID  = "worker-pod-uid-1"
		snapshotName  = "pause-snap-1"
		snapshotName2 = "pause-snap-2"
	)

	ateom := &fakeAteom{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeAteom(t, ateom)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	// A single "runsc" asset served from a fake bucket: enough to exercise the
	// content-addressed asset fetch without a gVisor release tarball.
	runsc := []byte("runsc binary")
	s := &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: runsc},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	sandboxAssets := &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   image,
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {
					Url:    "gs://test-bucket/runsc",
					Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
				},
			}},
		},
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxAssets:         sandboxAssets,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Pause: a deferred checkpoint, which leaves the snapshot on this node.
	resp, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, fidelity),
		UploadConfig:          deferredUpload(),
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	snapshotFile := filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName), "checkpoint.img")
	if _, err := os.Stat(snapshotFile); err != nil {
		t.Fatalf("pause did not write the local snapshot: %v", err)
	}
	st := resp.GetState()
	if got := st.GetLatestLocal(); got.GetSnapshot().GetSnapshotUuid() != snapshotName || got.GetLocalCommitTime() == nil {
		t.Errorf("latest_local = %v, want the pause snapshot with its commit time", got)
	}
	if st.GetLatestDurable() != nil || st.GetUploadDueTime() == nil {
		t.Errorf("state = %v, want nothing durable and an upload due", st)
	}

	// Resume: restores from that local snapshot.
	if _, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxAssets:         sandboxAssets,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, fidelity),
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := ateom.restored["checkpoint.img"]; got != "guest-memory" {
		t.Fatalf("restore staged %q for ateom, want the pause snapshot's %q", got, "guest-memory")
	}
	for _, rpc := range []string{"CheckpointWorkload", "RestoreWorkload"} {
		if got := ateom.fidelities[rpc]; got != toAteomFidelity(fidelity) {
			t.Errorf("%s carried fidelity %v, want %v", rpc, got, toAteomFidelity(fidelity))
		}
	}
	if !ateom.preserveRestoreDir {
		t.Errorf("RestoreWorkload preserve_restore_dir = false, want true for pure-local restore")
	}
	if entries, err := os.ReadDir(ateletpath.RestoreStateDir(actorUID)); err != nil || len(entries) != 0 {
		t.Fatalf("expected RestoreStateDir to remain empty on local pause restore, got entries=%v err=%v", entries, err)
	}

	// Second pause: must replace, not accumulate alongside, the first snapshot.
	if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName2, 2, fidelity),
		UploadConfig:          deferredUpload(),
	}); err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}
	snapshotFile2 := filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName2), "checkpoint.img")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(actorUID, snapshotName)); !os.IsNotExist(err) {
		t.Fatalf("first local snapshot survived second checkpoint: %v", err)
	}
	if _, err := os.Stat(snapshotFile2); err != nil {
		t.Fatalf("second pause did not write the local snapshot: %v", err)
	}

	// Terminate: the actor is gone, and so should its remaining snapshot be.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	// Every RPC hands ateom the same directory set, except that a local
	// restore points restore_dir at the pause snapshot; the fake already
	// relied on checkpoint_dir and restore_dir above to place and find it.
	for _, rpc := range []string{"RunWorkload", "CheckpointWorkload", "RestoreWorkload", "TerminateWorkload"} {
		want := ateletpath.ActorDirs(actorUID)
		if rpc == "RestoreWorkload" {
			want.RestoreDir = ateletpath.LocalSnapshotDir(actorUID, snapshotName)
		}
		if got := ateom.actorDirs[rpc]; !proto.Equal(got, want) {
			t.Errorf("%s carried actor actorDirs %v, want %v", rpc, got, want)
		}
	}

	localDir := ateletpath.LocalCheckpointsDir(actorUID)
	if _, err := os.Stat(localDir); !os.IsNotExist(err) {
		leaked, _ := filepath.Glob(filepath.Join(localDir, "*", "*"))
		t.Errorf("local checkpoint dir survived terminate (stat err = %v), leaked files: %v", err, leaked)
	}

	// Terminate is the only chance to reclaim the actor's directory: nothing
	// else on the node deletes it.
	actorDir := ateletpath.ActorPath(actorUID)
	if entries, err := os.ReadDir(actorDir); err == nil {
		left := make([]string, 0, len(entries))
		for _, e := range entries {
			left = append(left, e.Name())
		}
		t.Errorf("actor dir %s survived terminate with %d entries: %v", actorDir, len(left), left)
	} else if !os.IsNotExist(err) {
		t.Errorf("reading actor dir %s: %v", actorDir, err)
	}
}

// TestRestoreUsesRequestSandboxAssets checks that Restore runs the actor with
// the sandbox assets on the request, not the ones recorded in the snapshot
// manifest.
func TestRestoreUsesRequestSandboxAssets(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "actor-uid-1"
		workerPodUID = "worker-pod-uid-1"
		snapshotName = "pause-snap-1"
	)

	ateom := &fakeAteom{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeAteom(t, ateom)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))
	checkpointPause := host + "/pause:v1"
	pushTestImage(t, checkpointPause, singleFileLayer(t, "pause", "pause-v1"))
	restorePause := host + "/pause:v2"
	pushTestImage(t, restorePause, singleFileLayer(t, "pause", "pause-v2"))

	runsc := []byte("runsc binary")
	s := &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: runsc},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	assetsWithPause := func(pause string) *ateletpb.SandboxAssets {
		return &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   pause,
			Assets: map[string]*ateletpb.ArchAssets{
				runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
					runscAssetName: {
						Url:    "gs://test-bucket/runsc",
						Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
					},
				}},
			},
		}
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxAssets:         assetsWithPause(checkpointPause),
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	pauseSnapshot := lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              pauseSnapshot,
		UploadConfig:          deferredUpload(),
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	manifest, err := os.ReadFile(filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxManifestName))
	if err != nil {
		t.Fatalf("reading snapshot manifest: %v", err)
	}
	manifestRec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		t.Fatalf("unmarshalling snapshot manifest: %v", err)
	}
	if manifestRec.PauseImage != checkpointPause {
		t.Fatalf("manifest pause image = %q, want %q", manifestRec.PauseImage, checkpointPause)
	}

	if _, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxAssets:         assetsWithPause(restorePause),
		Spec:                  spec,
		Snapshot:              pauseSnapshot,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err := readSandboxRecord(actorUID)
	if err != nil {
		t.Fatalf("reading on-node sandbox record: %v", err)
	}
	if got.PauseImage != restorePause {
		t.Errorf("restored actor pause image = %q, want the request's %q", got.PauseImage, restorePause)
	}
}

// A failed activation preserves a live projection only until it starts resetting
// the actor's directories. Once reset deletes the files, their old registration
// must be fenced, including when reset itself or an early preparation step fails.
func TestActivationFailureBeforeRegistration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		restore     bool
		failure     string
		wantErr     string
		wantPresent bool
	}{
		{name: "Run asset failure preserves projection", failure: "asset", wantErr: "asset unavailable", wantPresent: true},
		{name: "Run partial reset removes registration", failure: "reset", wantErr: "while removing volume dir"},
		{name: "Run record failure removes registration", failure: "record", wantErr: "while recording sandbox assets"},
		{name: "Restore partial reset removes registration", restore: true, failure: "reset", wantErr: "while removing volume dir"},
		{name: "Restore manifest failure removes registration", restore: true, failure: "manifest", wantErr: "has no usable local copy and no uploaded copy"},
		{name: "Restore asset failure removes registration", restore: true, failure: "asset", wantErr: "asset unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			const actorUID, snapshotName = "actor-uid-1", "pause-snap-1"
			ctx := t.Context()
			store := newCTBStore(t)
			certA := string(testCertPEM(t))
			store.set(t, certA)
			refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
			spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
				Name: "trust", Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
			}}}
			registered, err := refresher.Register(actorUID, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, systemInfoVolumesFor(actorUID, spec))
			if err != nil {
				t.Fatal(err)
			}
			projectedPath := filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")
			if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certA {
				t.Fatalf("initial projection = %q, %v; want cert A", got, err)
			}

			content := []byte("runsc binary")
			assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
			if tc.restore && tc.failure != "manifest" {
				writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
					SandboxClass: "gvisor", PauseImage: testPauseImage,
					Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
					SnapshotFiles: []string{"checkpoint.img"},
				}, map[string]string{"checkpoint.img": "guest-memory"})
			}
			if tc.failure == "reset" {
				// Reset refuses to remove a populated external-volume directory,
				// but has already removed the system-info roots by then.
				dir := ateletpath.VolumeHostPath(actorUID, "mounted")
				if err := os.MkdirAll(dir, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("volume data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.failure == "record" {
				if err := os.MkdirAll(ateletpath.ActorSandboxAssetsFile(actorUID), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			storage := fakeObjectStorage{data: content}
			if tc.failure == "asset" {
				storage.err = fmt.Errorf("asset unavailable")
			}
			// The snapshot is in neither the node's nor this empty object
			// store.
			s := newPluginHerder(t, &recordingObjectStorage{})
			s.anonGCSClient, s.systemInfoVolumes = storage, refresher
			assets := &ateletpb.SandboxAssets{
				SandboxClass: "gvisor", PauseImage: testPauseImage,
				Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
					runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
				}}},
			}
			if tc.restore {
				_, err = s.Restore(ctx, &ateletpb.RestoreRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: "worker-pod-uid-1",
					SandboxAssets: assets, Spec: spec,
					Snapshot: lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
				})
			} else {
				_, err = s.Run(ctx, &ateletpb.RunRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: "worker-pod-uid-1",
					SandboxAssets: assets, Spec: spec,
				})
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("activation error = %v, want %q", err, tc.wantErr)
			}

			// Inspect disk and registry before changing the bundle hash: a later
			// rotation can hide a stale registration by recreating deleted files.
			if tc.wantPresent {
				if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certA {
					t.Errorf("pre-reset failure lost projection: %q, %v", got, err)
				}
				if refresher.actors[actorUID] != registered || registered.stale {
					t.Error("pre-reset failure invalidated the existing registration")
				}
			} else {
				if _, err := os.Stat(projectedPath); !os.IsNotExist(err) {
					t.Fatalf("reset did not delete prior projection: stat error = %v", err)
				}
				if got := refresher.actors[actorUID]; got != nil {
					t.Errorf("reset deleted projection but left registration live: %p", got)
				}
				if !registered.stale {
					t.Error("reset left the old registration available to an in-flight refresh")
				}
			}
			certB := string(testCertPEM(t))
			store.set(t, certB)
			if err := refresher.refreshBundle(ctx, trustbundle.EgressName); err != nil {
				t.Fatal(err)
			}
			if tc.wantPresent {
				if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certB {
					t.Errorf("preserved projection stopped refreshing: %q, %v", got, err)
				}
			} else if _, err := os.Stat(projectedPath); !os.IsNotExist(err) {
				t.Errorf("refresh recreated an invalidated projection: stat error = %v", err)
			}
		})
	}
}

func TestRunFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	s := &AteomHerder{
		anonGCSClient:     fakeObjectStorage{data: content},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
	}
	spec := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{{
			Name:   "trust",
			Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
		}},
	}
	_, err := s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:     "team-a",
		ActorName:    "actor-run",
		ActorUid:     "actor-uid-run",
		WorkerPodUid: "worker-pod-uid-1",
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   "://invalid-image",
			Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
			}}},
		},
		Spec: spec,
	})
	if err == nil || !strings.Contains(err.Error(), "while creating pause OCI bundle") {
		t.Fatalf("Run error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot("actor-uid-run", "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors["actor-uid-run"]; got != nil {
		t.Fatalf("failed Run left its registration live: %p", got)
	}
}

func TestRestoreFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	const (
		actorUID     = "actor-uid-restore-after"
		snapshotName = "pause-snap-after"
	)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    "://invalid-image",
		Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
		SnapshotFiles: []string{"checkpoint.img"},
	}, map[string]string{"checkpoint.img": "guest-memory"})

	spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
		Name:   "trust",
		Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
	}}}
	s := &AteomHerder{
		anonGCSClient:     fakeObjectStorage{data: content},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
	}
	_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:     "team-a",
		ActorName:    "actor-restore-after",
		ActorUid:     actorUID,
		WorkerPodUid: "worker-pod-uid-1",
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   "://invalid-image",
			Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
			}}},
		},
		Spec:     spec,
		Snapshot: lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	if err == nil || !strings.Contains(err.Error(), "while creating pause OCI bundle") {
		t.Fatalf("Restore error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors[actorUID]; got != nil {
		t.Fatalf("failed Restore left its owned registration live: %p", got)
	}
}

// TestTerminateWithoutWorkerPodUID verifies that Terminate with an empty
// WorkerPodUid (used when cleaning up the assigned node for a paused or
// crashed actor that no longer has a worker pod) skips dialing ateom while
// still pruning local checkpoints and removing actor directories on the node.
func TestTerminateWithoutWorkerPodUID(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "actor-uid-1"
		snapshotName = "pause-snap-1"
	)

	if err := resetActorDirs(actorUID); err != nil {
		t.Fatalf("resetActorDirs: %v", err)
	}
	snapshotDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
	if err := os.MkdirAll(snapshotDir, 0o700); err != nil {
		t.Fatalf("creating local snapshot dir: %v", err)
	}
	snapshotFile := filepath.Join(snapshotDir, "checkpoint.img")
	if err := os.WriteFile(snapshotFile, []byte("guest-memory"), 0o600); err != nil {
		t.Fatalf("writing local snapshot file: %v", err)
	}

	ateom := &fakeAteom{}
	serveFakeAteom(t, ateom)

	s := &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: "example.com/app:v1"}},
	}

	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "",
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	if got := ateom.actorDirs["TerminateWorkload"]; got != nil {
		t.Errorf("TerminateWorkload was called on ateom (%v), want skipped when WorkerPodUid is empty", got)
	}
	if _, err := os.Stat(ateletpath.LocalCheckpointsDir(actorUID)); !os.IsNotExist(err) {
		t.Errorf("local checkpoint dir survived terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorPath(actorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir survived terminate: %v", err)
	}
}

// pluginActorUID is the actor runPluginActor runs.
const pluginActorUID = "actor-uid-1"

// pluginActor is an actor running on a fake ateom, on a herder whose snapshot
// plugin stores into store.
type pluginActor struct {
	s       *AteomHerder
	ateom   *fakeAteom
	store   *recordingObjectStorage
	tracker *snapshotTracker
	assets  *ateletpb.SandboxAssets
	spec    *ateletpb.WorkloadSpec
}

// runPluginActor runs an actor whose snapshots atelet uploads uploadDelay
// after a deferred checkpoint, and detaches detachDelay after one.
func runPluginActor(t *testing.T, uploadDelay, detachDelay time.Duration) *pluginActor {
	t.Helper()
	useTempNodeDirs(t)

	a := &pluginActor{
		ateom: &fakeAteom{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}},
		store: &recordingObjectStorage{},
	}
	serveFakeAteom(t, a.ateom)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	runsc := []byte("runsc binary")
	a.s = newPluginHerder(t, a.store)
	a.s.ateomDialer = newAteomDialer(1)
	a.s.imageCache = newImageVolumeStore(t)
	a.s.anonGCSClient = fakeObjectStorage{data: runsc}
	a.s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
	a.tracker = useSnapshotTracker(t, a.s, uploadDelay, detachDelay)
	a.assets = &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   image,
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {
					Url:    "gs://test-bucket/runsc",
					Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
				},
			}},
		},
	}
	a.spec = &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	if _, err := a.s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "worker-pod-uid-1",
		SandboxAssets:         a.assets,
		Spec:                  a.spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return a
}

// snapshot is the Snapshot the control plane names for the actor's checkpoint
// at generation.
func (a *pluginActor) snapshot(name string, generation int32) *ateletpb.Snapshot {
	return lifecycleSnapshot(pluginActorUID, name, generation, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
}

func (a *pluginActor) checkpoint(t *testing.T, snap *ateletpb.Snapshot, mode ateletpb.UploadMode) (*ateletpb.CheckpointResponse, error) {
	t.Helper()
	return a.s.Checkpoint(t.Context(), &ateletpb.CheckpointRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "worker-pod-uid-1",
		Spec:                  a.spec,
		Snapshot:              snap,
		UploadConfig:          &ateletpb.UploadConfig{Mode: mode},
	})
}

func (a *pluginActor) restore(t *testing.T, snap *ateletpb.Snapshot) error {
	t.Helper()
	_, err := a.s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "worker-pod-uid-1",
		SandboxAssets:         a.assets,
		Spec:                  a.spec,
		Snapshot:              snap,
	})
	return err
}

func (a *pluginActor) promote(t *testing.T, snap *ateletpb.Snapshot) (*ateletpb.PromoteSnapshotResponse, error) {
	t.Helper()
	return a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		Snapshot:              snap,
	})
}

func (a *pluginActor) detach(t *testing.T) (*ateletpb.DetachActorResponse, error) {
	t.Helper()
	return a.s.DetachActor(t.Context(), &ateletpb.DetachActorRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
	})
}

// waitDurable waits until no upload is in flight and atelet reports a snapshot
// at generation or later durable, and returns the actor's state.
func (a *pluginActor) waitDurable(t *testing.T, generation int32) *ateletpb.ActorSnapshotState {
	t.Helper()
	var st *ateletpb.ActorSnapshotState
	eventually(t, fmt.Sprintf("a snapshot at generation %d is durable", generation), func() bool {
		st = a.tracker.state(pluginActorUID)
		return st.GetUploading() == nil && st.GetLatestDurable().GetSnapshot().GetGeneration() >= generation
	})
	return st
}

// manifestKey is where the object store keeps the manifest of the actor's
// snapshot named name.
func manifestKey(name string) string {
	return "test-bucket/atespaces/ate-demo/actors/" + pluginActorUID + "/snapshots/" + name + "/" + sandboxManifestName
}

// TestImmediateCheckpointKeepsNoLocalCopy suspends a running actor: the
// checkpoint uploads the snapshot before it returns and leaves none on the
// node. A retry returns the same state without checkpointing again, and a
// request from a stale view of the actor is refused.
func TestImmediateCheckpointKeepsNoLocalCopy(t *testing.T) {
	a := runPluginActor(t, time.Hour, time.Hour)
	snap := a.snapshot("suspend-snap-1", 1)

	resp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !slices.Contains(a.store.keys(), manifestKey("suspend-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}
	if _, err := os.Stat(ateletpath.LocalCheckpointsDir(pluginActorUID)); !os.IsNotExist(err) {
		t.Errorf("local checkpoints after an immediate checkpoint: stat = %v, want none", err)
	}
	st := resp.GetState()
	if st.GetLatestLocal() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want no local snapshot and no upload due", st)
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with its upload time", got, snap)
	}

	// A retry, as when the response was lost, answers from atelet's state.
	a.ateom.actorDirs = nil
	retry, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if err != nil {
		t.Fatalf("retried Checkpoint: %v", err)
	}
	if !proto.Equal(retry.GetState(), st) {
		t.Errorf("retried Checkpoint state = %v, want %v", retry.GetState(), st)
	}

	// Another snapshot at the same generation comes from a stale view.
	_, err = a.checkpoint(t, a.snapshot("suspend-snap-2", 1), ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if got := apierror.Code(err); got != codes.Aborted {
		t.Errorf("stale Checkpoint code = %v (err %v), want Aborted", got, err)
	}
	if got := a.ateom.actorDirs["CheckpointWorkload"]; got != nil {
		t.Errorf("a retried or stale Checkpoint called CheckpointWorkload (%v), want not", got)
	}
}

// TestDeferredCheckpointUploadsInBackground pauses an actor: the checkpoint
// keeps the snapshot on the node, and atelet uploads it once it is due. The
// local copy stays, and a resume restores from it.
func TestDeferredCheckpointUploadsInBackground(t *testing.T) {
	a := runPluginActor(t, 0, time.Hour)
	snap := a.snapshot("pause-snap-1", 1)

	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	st := a.waitDurable(t, 1)
	if !slices.Contains(a.store.keys(), manifestKey("pause-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with its upload time", got, snap)
	}
	if got := st.GetLatestLocal(); !proto.Equal(got.GetSnapshot(), snap) {
		t.Errorf("latest_local = %v, want %v kept after its upload", got, snap)
	}
	if st.GetUploadDueTime() != nil {
		t.Errorf("upload_due_time = %v, want unset once the snapshot is durable", st.GetUploadDueTime())
	}

	// Empty the object store to show that the resume reads the local copy.
	a.store.mu.Lock()
	a.store.objects = nil
	a.store.mu.Unlock()
	if err := a.restore(t, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := a.ateom.restored["checkpoint.img"]; got != "guest-memory" {
		t.Errorf("restore staged %q for ateom, want %q", got, "guest-memory")
	}
	if !a.ateom.preserveRestoreDir {
		t.Error("PreserveRestoreDir = false, want true for a restore from the local copy")
	}
}

// TestPromoteSnapshotAfterPause promotes a paused actor's snapshot before
// atelet's background upload: the snapshot becomes durable in object storage
// while its local copy stays on the node for same-node Restore.
func TestPromoteSnapshotAfterPause(t *testing.T) {
	a := runPluginActor(t, time.Hour, time.Hour)
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	resp, err := a.promote(t, snap)
	if err != nil {
		t.Fatalf("PromoteSnapshot: %v", err)
	}
	if !slices.Contains(a.store.keys(), manifestKey("pause-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}
	st := resp.GetState()
	if got := st.GetLatestLocal(); !proto.Equal(got.GetSnapshot(), snap) {
		t.Errorf("latest_local = %v, want %v kept locally", got, snap)
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with its upload time", got, snap)
	}

	// Empty the object store to show that the resume still reads the local copy.
	a.store.mu.Lock()
	a.store.objects = nil
	a.store.mu.Unlock()
	if err := a.restore(t, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !a.ateom.preserveRestoreDir {
		t.Error("PreserveRestoreDir = false, want true for a restore from the local copy")
	}
}

// TestDetachActorAfterPause suspends a paused actor with DetachActor, before
// and after atelet's background upload. Either way the snapshot ends up
// durable and off the node, a retry changes nothing, and a resume reads object
// storage.
func TestDetachActorAfterPause(t *testing.T) {
	for _, tc := range []struct {
		name string
		// uploadedFirst lets the background upload finish before the detach.
		uploadedFirst bool
	}{
		{name: "before the background upload"},
		{name: "after the background upload", uploadedFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uploadDelay := time.Hour
			if tc.uploadedFirst {
				uploadDelay = 0
			}
			a := runPluginActor(t, uploadDelay, time.Hour)
			snap := a.snapshot("pause-snap-1", 1)
			if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if tc.uploadedFirst {
				a.waitDurable(t, 1)
			}
			puts := len(a.store.uploads())

			resp, err := a.detach(t)
			if err != nil {
				t.Fatalf("DetachActor: %v", err)
			}
			uploaded := a.store.uploads()[puts:]
			switch {
			case tc.uploadedFirst && len(uploaded) != 0:
				t.Errorf("DetachActor uploaded %v, want nothing: the snapshot was already durable", uploaded)
			case !tc.uploadedFirst && !slices.Contains(uploaded, manifestKey("pause-snap-1")):
				t.Errorf("DetachActor uploaded %v, want the snapshot", uploaded)
			}
			if _, err := os.Stat(ateletpath.LocalCheckpointsDir(pluginActorUID)); !os.IsNotExist(err) {
				t.Errorf("local checkpoints after DetachActor: stat = %v, want none", err)
			}
			st := resp.GetState()
			if st.GetLatestLocal() != nil || st.GetUploadDueTime() != nil {
				t.Errorf("state = %v, want no local snapshot and no upload due", st)
			}
			if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
				t.Errorf("latest_durable = %v, want %v with its upload time", got, snap)
			}

			puts = len(a.store.uploads())
			if _, err := a.detach(t); err != nil {
				t.Fatalf("retried DetachActor: %v", err)
			}
			if got := a.store.uploads()[puts:]; len(got) != 0 {
				t.Errorf("retried DetachActor uploaded %v, want nothing", got)
			}

			if err := a.restore(t, snap); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if got, want := a.ateom.actorDirs["RestoreWorkload"].GetRestoreDir(), ateletpath.RestoreStateDir(pluginActorUID); got != want {
				t.Errorf("restore dir = %q, want the uploaded copy staged in %q", got, want)
			}
			if got := a.ateom.restored["checkpoint.img"]; got != "guest-memory" {
				t.Errorf("restore staged %q for ateom, want %q", got, "guest-memory")
			}
		})
	}
}

// TestDetachTimerDetachesPausedActor leaves a paused actor on the node past its
// detach delay: atelet makes the snapshot durable and deletes it from the node
// on its own, and a resume then reads object storage.
func TestDetachTimerDetachesPausedActor(t *testing.T) {
	a := runPluginActor(t, time.Hour, 0)
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	eventually(t, "atelet detaches the actor", func() bool {
		st := a.tracker.state(pluginActorUID)
		_, err := os.Stat(ateletpath.LocalCheckpointsDir(pluginActorUID))
		return st.GetLatestLocal() == nil && st.GetLatestDurable() != nil && os.IsNotExist(err)
	})
	if !slices.Contains(a.store.keys(), manifestKey("pause-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}

	if err := a.restore(t, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got, want := a.ateom.actorDirs["RestoreWorkload"].GetRestoreDir(), ateletpath.RestoreStateDir(pluginActorUID); got != want {
		t.Errorf("restore dir = %q, want the uploaded copy staged in %q", got, want)
	}
}

// TestRestoreFallsBackFromUnusableLocalSnapshot breaks the local copy of a
// paused actor's snapshot the way a node crash can, and checks that Restore
// stages the uploaded copy instead, or fails as lost when there is none.
func TestRestoreFallsBackFromUnusableLocalSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		// corrupt breaks the local copy in localDir.
		corrupt func(t *testing.T, localDir string)
		// notUploaded restores before atelet uploads the snapshot.
		notUploaded bool
	}{
		{name: "manifest does not parse", corrupt: func(t *testing.T, localDir string) {
			if err := os.WriteFile(filepath.Join(localDir, sandboxManifestName), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "listed file is missing", corrupt: func(t *testing.T, localDir string) {
			if err := os.Remove(filepath.Join(localDir, "checkpoint.img")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "not uploaded", notUploaded: true, corrupt: func(t *testing.T, localDir string) {
			if err := os.WriteFile(filepath.Join(localDir, sandboxManifestName), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uploadDelay := time.Duration(0)
			if tc.notUploaded {
				uploadDelay = time.Hour
			}
			a := runPluginActor(t, uploadDelay, time.Hour)
			snap := a.snapshot("pause-snap-1", 1)
			if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if !tc.notUploaded {
				a.waitDurable(t, 1)
			}
			tc.corrupt(t, ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1"))

			err := a.restore(t, snap)
			if tc.notUploaded {
				if got := apierror.Code(err); got != codes.NotFound {
					t.Fatalf("Restore code = %v (err %v), want NotFound: the snapshot is lost", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if got, want := a.ateom.actorDirs["RestoreWorkload"].GetRestoreDir(), ateletpath.RestoreStateDir(pluginActorUID); got != want {
				t.Errorf("restore dir = %q, want the uploaded copy staged in %q", got, want)
			}
			if a.ateom.preserveRestoreDir {
				t.Error("PreserveRestoreDir = true, want false for a copy staged from object storage")
			}
			if got := a.ateom.restored["checkpoint.img"]; got != "guest-memory" {
				t.Errorf("restore staged %q for ateom, want %q", got, "guest-memory")
			}
		})
	}
}
