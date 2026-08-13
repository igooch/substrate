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

// Package imagecache asserts the node-local image cache's GC contract
// end to end: the bundle-spec root set tracks the actor lifecycle, and
// startup recovery reclaims orphans but fails toward retention when the
// pool cannot be fully enumerated. Node-filesystem truth is checked
// through a probe pod that mounts the ateom base path; log assertions
// use the loop's caller-side lines, which are the stable contract
// (per-item lines are Debug and Kind-only).
//
// These scenarios deliberately need no GC flag changes: eviction under
// pressure requires aggressive flags on a shared cluster's DaemonSet
// and is left to a dedicated job (see the PR discussion).
package imagecache

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	ateSystemNS = "ate-system"
	// probeMount is where the probe pod mounts the node's ateom base path
	// (hostPath /var/lib/ateom-gvisor).
	probeMount = "/ateom"
	cacheDir   = probeMount + "/image-cache"
	actorsDir  = probeMount + "/actors"

	// Planted names: a well-formed but unreferenced layer digest, a
	// non-digest dir the sweeps must leave alone, and a record that
	// cannot decode (gates every pass until removed).
	orphanHex   = "9999999999999999999999999999999999999999999999999999999999999999"
	bogusDir    = "not-a-layer-digest"
	badRecord   = "1111111111111111111111111111111111111111111111111111111111111111.json"
	orphanDir   = cacheDir + "/layers/sha256/" + orphanHex
	bogusPath   = cacheDir + "/layers/sha256/" + bogusDir
	badRecPath  = cacheDir + "/manifests/sha256/" + badRecord
	versionFile = cacheDir + "/version"
)

// TestRootSetLifecycle pins the eviction root set's authority end to end:
// a running actor's bundle spec exists on its node (rooting its image),
// suspend removes it (un-rooting), and resume writes it back.
func TestRootSetLifecycle(t *testing.T) {
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)
	ctx := t.Context()
	atespace := nsObj.Name

	at := createFixture(ctx, t, clients, nsObj)
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	const actorName = "rootset"
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() { deleteActor(t, clients, atespace, actorName) })
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: actorName}

	// New actors land SUSPENDED; resume places and runs them.
	waitForActorStatus(t, clients, ref, ateapipb.Actor_STATUS_SUSPENDED, 90*time.Second)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	waitForActorStatus(t, clients, ref, ateapipb.Actor_STATUS_RUNNING, 120*time.Second)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	uid := actor.GetMetadata().GetUid()
	specGlob := actorsDir + "/" + uid + "/bundles/*/rootfs-overlay.json"

	probe := startProbe(ctx, t, clients, nsObj.Name, workerNode(ctx, t, clients, nsObj.Name))

	// Running: the spec roots the image on this node.
	if probe.rc(t, "ls "+specGlob) != 0 {
		t.Fatalf("running actor %s has no bundle overlay spec on its node (root set would not protect it)", uid)
	}

	// Suspend tears the bundle down: the spec goes, un-rooting the image.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorStatus(t, clients, ref, ateapipb.Actor_STATUS_SUSPENDED, 120*time.Second)
	specDeadline := time.Now().Add(30 * time.Second)
	for probe.rc(t, "ls "+specGlob) == 0 {
		if time.Now().After(specDeadline) {
			t.Fatalf("suspended actor %s still has bundle overlay specs (would root its image forever)", uid)
		}
		time.Sleep(1 * time.Second)
	}

	// Resume re-places it; the spec is rewritten before any mount.
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor (second): %v", err)
	}
	waitForActorStatus(t, clients, ref, ateapipb.Actor_STATUS_RUNNING, 120*time.Second)
	if probe.rc(t, "ls "+specGlob) != 0 {
		t.Fatalf("resumed actor %s has no bundle overlay spec", uid)
	}
}

// TestStartupRecoveryArc pins startup recovery's two halves on a real
// node: with an undecodable record planted, the scan is gated and
// reclaims nothing (fail toward retention — the planted orphan must
// survive); with the record removed, the next startup reclaims the
// orphan, leaves the non-digest dir alone, and keeps the layout marker.
func TestStartupRecoveryArc(t *testing.T) {
	if os.Getenv("E2E_IMAGECACHE_DISRUPTIVE") == "" {
		t.Skip("restarts the node's atelet; run alone with E2E_IMAGECACHE_DISRUPTIVE=1 (see the dedicated CI step)")
	}
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)
	ctx := t.Context()

	node, atelet := anyAteletPod(ctx, t, clients)
	probe := startProbe(ctx, t, clients, nsObj.Name, node)

	// Self-heal first: a killed earlier run cannot invoke t.Cleanup, and
	// a leftover undecodable record would gate this node's GC forever.
	probe.exec(t, fmt.Sprintf("rm -rf %s %s %s", badRecPath, orphanDir, bogusPath))

	// Plant: an aged orphan layer (well-formed digest dir, no record), a
	// non-digest dir, and a record that cannot decode. Backdating clears
	// the min-age veto; the size file must be planted too, or the GC
	// loop's CacheSize backfill writes one into the dir and its
	// bump-don't-restore semantics re-freshen the mtime, re-arming the
	// veto for the next scan.
	probe.exec(t, fmt.Sprintf(
		"mkdir -p %[1]s/fs && echo junk > %[1]s/fs/junk && printf 5 > %[1]s/size && touch -d '2020-01-01 00:00:00' %[1]s && mkdir -p %[2]s && printf '{not json' > %[3]s",
		orphanDir, bogusPath, badRecPath))
	t.Cleanup(func() { // never leave a gate planted on a shared cluster
		probe.exec(t, fmt.Sprintf("rm -rf %s %s %s", badRecPath, orphanDir, bogusPath))
	})
	if probe.rc(t, "test -f "+badRecPath+" && test -d "+orphanDir) != 0 {
		t.Fatal("plants missing after setup")
	}

	// Restart 1: the bad record gates the startup scan; nothing reclaimed.
	atelet = restartAtelet(ctx, t, clients, node, atelet)
	logs := waitForLog(ctx, t, clients, atelet, "Image cache startup orphan scan skipped", 60*time.Second)
	if strings.Contains(logs, "startup scan reclaimed orphan layers") {
		t.Fatal("gated startup scan still reclaimed layers")
	}
	if probe.rc(t, "test -d "+orphanDir) != 0 {
		t.Fatal("orphan layer was removed during a gated scan (must fail toward retention)")
	}

	// Restart 2: gate removed; the orphan is reclaimed, hygiene holds.
	// Filesystem truth is the contract here — the INFO reclaim line can
	// land in a pod whose logs a container-level restart carries away —
	// so poll the node, and use logs only for the must-not-gate check.
	probe.exec(t, "rm "+badRecPath)
	atelet = restartAtelet(ctx, t, clients, node, atelet)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if probe.rc(t, "test -d "+orphanDir) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orphan layer survived a clean startup scan")
		}
		time.Sleep(2 * time.Second)
	}
	if logs := ateletLogs(ctx, t, clients, atelet); strings.Contains(logs, "Image cache startup orphan scan skipped") {
		t.Fatal("startup scan still gated after the bad record was removed")
	}
	if probe.rc(t, "test -d "+bogusPath) != 0 {
		t.Fatal("non-digest dir was removed (sweeps must not delete operator files)")
	}
	if probe.rc(t, "test -s "+versionFile) != 0 {
		t.Fatal("cache layout version marker missing after recovery")
	}
	// The steady-state contract: no gated-pass ERRORs once the pool is
	// clean (the immediate first pass ran at startup).
	if logs := ateletLogs(ctx, t, clients, atelet); strings.Contains(logs, "Image cache GC pass skipped") {
		t.Fatal("clean pool still logged a gated GC pass")
	}
}

// --- helpers ---

// workerNode returns the node running the test namespace's worker pod
// (the fixture pool has exactly one).
func workerNode(ctx context.Context, t *testing.T, clients *e2e.Clients, ns string) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		pods, err := clients.K8s.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err == nil {
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning && p.Spec.NodeName != "" {
					return p.Spec.NodeName
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatal("no running worker pod found in test namespace")
	return ""
}

// anyAteletPod returns one atelet pod and its node.
func anyAteletPod(ctx context.Context, t *testing.T, clients *e2e.Clients) (node, pod string) {
	t.Helper()
	pods, err := clients.K8s.CoreV1().Pods(ateSystemNS).List(ctx, metav1.ListOptions{LabelSelector: "app=atelet"})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("listing atelet pods: %v (found %d)", err, len(pods.Items))
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning {
			return p.Spec.NodeName, p.Name
		}
	}
	t.Fatal("no Running atelet pod")
	return "", ""
}

// restartAtelet deletes the atelet pod on node and waits for the
// DaemonSet's replacement to be Ready, returning the new pod name.
func restartAtelet(ctx context.Context, t *testing.T, clients *e2e.Clients, node, oldPod string) string {
	t.Helper()
	if err := clients.K8s.CoreV1().Pods(ateSystemNS).Delete(ctx, oldPod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting atelet pod %s: %v", oldPod, err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		pods, err := clients.K8s.CoreV1().Pods(ateSystemNS).List(ctx, metav1.ListOptions{LabelSelector: "app=atelet"})
		if err == nil {
			for _, p := range pods.Items {
				if p.Spec.NodeName != node || p.Name == oldPod {
					continue
				}
				for _, c := range p.Status.Conditions {
					if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
						return p.Name
					}
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("atelet did not come back Ready on node %s", node)
	return ""
}

func ateletLogs(ctx context.Context, t *testing.T, clients *e2e.Clients, pod string) string {
	t.Helper()
	raw, err := clients.K8s.CoreV1().Pods(ateSystemNS).GetLogs(pod, &corev1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		t.Fatalf("reading logs of %s: %v", pod, err)
	}
	return string(raw)
}

// waitForLog polls the pod's logs until they contain want, returning the
// full log text.
func waitForLog(ctx context.Context, t *testing.T, clients *e2e.Clients, pod, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var logs string
	for time.Now().Before(deadline) {
		logs = ateletLogs(ctx, t, clients, pod)
		if strings.Contains(logs, want) {
			return logs
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("log line %q did not appear in %s within %v", want, pod, timeout)
	return ""
}

// probe is a pod on a specific node with the ateom base path mounted:
// the suite's window onto node-filesystem truth (atelet's own image may
// not carry a shell, and docker exec would be Kind-only).
type probe struct {
	ns, name string
}

func startProbe(ctx context.Context, t *testing.T, clients *e2e.Clients, ns, node string) *probe {
	t.Helper()
	hostPathType := corev1.HostPathDirectory
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "imagecache-probe", Namespace: ns},
		Spec: corev1.PodSpec{
			NodeName:      node,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   "mirror.gcr.io/library/busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e",
				Command: []string{"sleep", "3600"},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "ateom", MountPath: probeMount},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "ateom",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/ateom-gvisor",
					Type: &hostPathType,
				}},
			}},
		},
	}
	if _, err := clients.K8s.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating probe pod: %v", err)
	}
	t.Cleanup(func() {
		_ = clients.K8s.CoreV1().Pods(ns).Delete(context.Background(), p.Name, metav1.DeleteOptions{})
	})
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := clients.K8s.CoreV1().Pods(ns).Get(ctx, p.Name, metav1.GetOptions{})
		if err == nil && cur.Status.Phase == corev1.PodRunning {
			return &probe{ns: ns, name: p.Name}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatal("probe pod did not reach Running")
	return nil
}

// exec runs a shell command in the probe pod via kubectl (the repo's
// e2e idiom for exec) and returns combined output.
func (p *probe) exec(t *testing.T, cmd string) string {
	t.Helper()
	args := []string{"exec", "-n", p.ns, p.name}
	if e2e.KubeContext != "" {
		args = append([]string{"--context=" + e2e.KubeContext}, args...)
	}
	if e2e.KubeConfig != "" {
		args = append([]string{"--kubeconfig=" + e2e.KubeConfig}, args...)
	}
	args = append(args, "--", "sh", "-c", cmd)
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	if err != nil {
		// Callers use shell tests that legitimately exit non-zero; only
		// they can judge, so hand back the output either way.
		t.Logf("probe exec %q: %v (output: %s)", cmd, err, out)
	}
	return string(out)
}

// rc runs cmd in the probe and returns its shell exit status; a
// kubectl transport failure fails the test instead of masquerading as
// a filesystem result.
func (p *probe) rc(t *testing.T, cmd string) int {
	t.Helper()
	out := p.exec(t, "("+cmd+") >/dev/null 2>&1; echo rc=$?")
	m := strings.LastIndex(out, "rc=")
	if m < 0 {
		t.Fatalf("probe exec transport failed for %q: %s", cmd, out)
	}
	n, err := strconv.Atoi(strings.TrimSpace(out[m+3:]))
	if err != nil {
		t.Fatalf("unparseable rc from probe for %q: %s", cmd, out)
	}
	return n
}

func waitForActorStatus(t *testing.T, clients *e2e.Clients, ref *ateapipb.ObjectRef, want ateapipb.Actor_Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last ateapipb.Actor_Status
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: ref})
		if err == nil {
			last = resp.GetStatus()
			if last == want {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("actor %s/%s never reached %v (last: %v)", ref.GetAtespace(), ref.GetName(), want, last)
}

// deleteActor suspends (if needed) then deletes; for t.Cleanup, so it
// uses Background and tolerates already-gone.
func deleteActor(t *testing.T, clients *e2e.Clients, atespace, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		return
	}
	if resp.GetStatus() == ateapipb.Actor_STATUS_RUNNING {
		if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err == nil {
			waitForActorStatus(t, clients, ref, ateapipb.Actor_STATUS_SUSPENDED, 120*time.Second)
		}
	}
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
		t.Logf("DeleteActor %s: %v", name, err)
	}
}

// createFixture provisions a 1-worker pool and template copying the
// installed counter demo's resolved runtime (the demo/parking pattern).
func createFixture(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace) *v1alpha1.ActorTemplate {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	srcNS, srcName := "ate-demo-counter", "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		srcNS = v
	}
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		srcName = v
	}
	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source WorkerPool %s/%s: %v", srcNS, srcName, err)
	}
	existingAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source ActorTemplate %s/%s: %v", srcNS, srcName, err)
	}
	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "imagecache", Namespace: nsObj.Name, Labels: map[string]string{"demo": nsObj.Name}},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          1,
			AteomImage:        existingWp.Spec.AteomImage,
			SandboxClass:      existingWp.Spec.SandboxClass,
			SandboxConfigName: existingWp.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}
	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "imagecache", Namespace: nsObj.Name},
		Spec: v1alpha1.ActorTemplateSpec{
			WorkerSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"demo": nsObj.Name}},
			SandboxClass:   existingAt.Spec.SandboxClass,
			Containers:     existingAt.Spec.Containers,
			SnapshotsConfig: v1alpha1.SnapshotsConfig{
				Location: "gs://" + env["BUCKET_NAME"] + "/e2e-imagecache-" + nsObj.Name,
			},
			Volumes: existingAt.Spec.Volumes,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Create(ctx, at, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create ActorTemplate: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	var lastPhase v1alpha1.PhaseType
	for time.Now().Before(deadline) {
		curAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Get(ctx, at.Name, metav1.GetOptions{})
		if err == nil {
			lastPhase = curAt.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				return at
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s transitioned to PhaseFailed", at.Name)
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for ActorTemplate %q to be Ready (last phase: %s)", at.Name, lastPhase)
	return nil
}
