/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package prober

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/probe"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func init() {
}

var defaultProbe = &v1.Probe{
	ProbeHandler: v1.ProbeHandler{
		Exec: &v1.ExecAction{},
	},
	TimeoutSeconds:   1,
	PeriodSeconds:    1,
	SuccessThreshold: 1,
	FailureThreshold: 3,
}

func TestAddRemovePods(t *testing.T) {
	ctx := ktesting.Init(t)
	noProbePod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "no_probe_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "no_probe1",
			}, {
				Name: "no_probe2",
			}, {
				Name: "no_probe3",
			}},
		},
	}

	probePod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "probe_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "probe1",
			}, {
				Name:           "readiness",
				ReadinessProbe: defaultProbe,
			}, {
				Name: "probe2",
			}, {
				Name:          "liveness",
				LivenessProbe: defaultProbe,
			}, {
				Name: "probe3",
			}, {
				Name:         "startup",
				StartupProbe: defaultProbe,
			}},
		},
	}

	m := newTestManager()
	defer cleanup(t, m)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// Adding a pod with no probes should be a no-op.
	m.AddPod(ctx, &noProbePod)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// Adding a pod with probes.
	m.AddPod(ctx, &probePod)
	probePaths := map[types.UID][]containerProbeKey{
		"probe_pod": {
			{"readiness", readiness},
			{"liveness", liveness},
			{"startup", startup},
		},
	}
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// Removing un-probed pod.
	m.RemovePod(&noProbePod)
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// Removing probed pod.
	m.RemovePod(&probePod)
	if err := waitForWorkerExit(t, m, probePaths); err != nil {
		t.Fatal(err)
	}
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// Removing already removed pods should be a no-op.
	m.RemovePod(&probePod)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}
}

func TestAddPodContinuesAfterExistingWorker(t *testing.T) {
	ctx := ktesting.Init(t)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "test_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:           "container_a",
					ReadinessProbe: defaultProbe,
				},
				{
					Name:           "container_b",
					ReadinessProbe: defaultProbe,
				},
			},
		},
	}

	m := newTestManager()
	defer cleanup(t, m)

	// First AddPod: registers workers for both containers.
	m.AddPod(ctx, &pod)
	if err := expectProbes(m, map[types.UID][]containerProbeKey{
		"test_pod": {
			{"container_a", readiness},
			{"container_b", readiness},
		},
	}); err != nil {
		t.Fatalf("after first AddPod: %v", err)
	}

	// Simulate container_b's worker being removed while container_a's is still present.
	m.workerLock.Lock()
	if podWorkers, ok := m.workers["test_pod"]; ok {
		delete(podWorkers, containerProbeKey{"container_b", readiness})
	}
	m.workerLock.Unlock()

	// Second AddPod: should re-register container_b's missing worker.
	// Previously, hitting container_a's existing worker caused an early return,
	// so container_b was never re-registered.
	m.AddPod(ctx, &pod)

	if err := expectProbes(m, map[types.UID][]containerProbeKey{
		"test_pod": {
			{"container_a", readiness},
			{"container_b", readiness},
		},
	}); err != nil {
		t.Errorf("container_b worker was not re-registered after second AddPod: %v", err)
	}
}

func TestAddRemovePodsWithRestartableInitContainer(t *testing.T) {
	m := newTestManager()
	defer cleanup(t, m)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	testCases := []struct {
		desc                        string
		probePaths                  map[types.UID][]containerProbeKey
		hasRestartableInitContainer bool
	}{
		{
			desc:                        "pod without sidecar",
			probePaths:                  nil,
			hasRestartableInitContainer: false,
		},
		{
			desc: "pod with sidecar",
			probePaths: map[types.UID][]containerProbeKey{
				"restartable_init_container_pod": {
					{"restartable-init", liveness},
					{"restartable-init", readiness},
					{"restartable-init", startup},
				},
			},
			hasRestartableInitContainer: true,
		},
	}

	containerRestartPolicy := func(hasRestartableInitContainer bool) *v1.ContainerRestartPolicy {
		if !hasRestartableInitContainer {
			return nil
		}
		restartPolicy := v1.ContainerRestartPolicyAlways
		return &restartPolicy
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			probePod := v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "restartable_init_container_pod",
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{{
						Name: "init",
					}, {
						Name:           "restartable-init",
						LivenessProbe:  defaultProbe,
						ReadinessProbe: defaultProbe,
						StartupProbe:   defaultProbe,
						RestartPolicy:  containerRestartPolicy(tc.hasRestartableInitContainer),
					}},
					Containers: []v1.Container{{
						Name: "main",
					}},
				},
			}

			// Adding a pod with probes.
			m.AddPod(ctx, &probePod)
			if err := expectProbes(m, tc.probePaths); err != nil {
				t.Error(err)
			}

			// Removing probed pod.
			m.RemovePod(&probePod)
			if err := waitForWorkerExit(t, m, tc.probePaths); err != nil {
				t.Fatal(err)
			}
			if err := expectProbes(m, nil); err != nil {
				t.Error(err)
			}

			// Removing already removed pods should be a no-op.
			m.RemovePod(&probePod)
			if err := expectProbes(m, nil); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCleanupPods(t *testing.T) {
	ctx := ktesting.Init(t)
	m := newTestManager()
	defer cleanup(t, m)
	podToCleanup := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod_cleanup",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
			}, {
				Name:          "prober2",
				LivenessProbe: defaultProbe,
			}, {
				Name:         "prober3",
				StartupProbe: defaultProbe,
			}},
		},
	}
	podToKeep := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod_keep",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
			}, {
				Name:          "prober2",
				LivenessProbe: defaultProbe,
			}, {
				Name:         "prober3",
				StartupProbe: defaultProbe,
			}},
		},
	}
	m.AddPod(ctx, &podToCleanup)
	m.AddPod(ctx, &podToKeep)

	desiredPods := map[types.UID]sets.Empty{}
	desiredPods[podToKeep.UID] = sets.Empty{}
	m.CleanupPods(desiredPods)

	removedProbes := map[types.UID][]containerProbeKey{
		"pod_cleanup": {
			{"prober1", readiness},
			{"prober2", liveness},
			{"prober3", startup},
		},
	}
	expectedProbes := map[types.UID][]containerProbeKey{
		"pod_keep": {
			{"prober1", readiness},
			{"prober2", liveness},
			{"prober3", startup},
		},
	}
	if err := waitForWorkerExit(t, m, removedProbes); err != nil {
		t.Fatal(err)
	}
	if err := expectProbes(m, expectedProbes); err != nil {
		t.Error(err)
	}
}

func TestCleanupRepeated(t *testing.T) {
	ctx := ktesting.Init(t)
	m := newTestManager()
	defer cleanup(t, m)
	podTemplate := v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
				LivenessProbe:  defaultProbe,
				StartupProbe:   defaultProbe,
			}},
		},
	}

	const numTestPods = 100
	for i := 0; i < numTestPods; i++ {
		pod := podTemplate
		pod.UID = types.UID(strconv.Itoa(i))
		m.AddPod(ctx, &pod)
	}

	for i := 0; i < 10; i++ {
		m.CleanupPods(map[types.UID]sets.Empty{})
	}
}

func TestUpdatePodStatus(t *testing.T) {
	ctx := ktesting.Init(t)
	logger := ctx.Logger()
	unprobed := v1.ContainerStatus{
		Name:        "unprobed_container",
		ContainerID: "test://unprobed_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedReady := v1.ContainerStatus{
		Name:        "probed_container_ready",
		ContainerID: "test://probed_container_ready_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedPending := v1.ContainerStatus{
		Name:        "probed_container_pending",
		ContainerID: "test://probed_container_pending_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedUnready := v1.ContainerStatus{
		Name:        "probed_container_unready",
		ContainerID: "test://probed_container_unready_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	notStartedNoReadiness := v1.ContainerStatus{
		Name:        "not_started_container_no_readiness",
		ContainerID: "test://not_started_container_no_readiness_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	startedNoReadiness := v1.ContainerStatus{
		Name:        "started_container_no_readiness",
		ContainerID: "test://started_container_no_readiness_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	terminated := v1.ContainerStatus{
		Name:        "terminated_container",
		ContainerID: "test://terminated_container_id",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{},
		},
	}
	podStatus := v1.PodStatus{
		Phase: v1.PodRunning,
		ContainerStatuses: []v1.ContainerStatus{
			unprobed, probedReady, probedPending, probedUnready, notStartedNoReadiness, startedNoReadiness, terminated,
		},
	}

	m := newTestManager()
	// no cleanup: using fake workers.

	// Setup probe "workers" and cached results.
	m.workers = map[types.UID]map[containerProbeKey]*worker{
		testPodUID: {
			{unprobed.Name, liveness}:             {},
			{probedReady.Name, readiness}:         {},
			{probedPending.Name, readiness}:       {},
			{probedUnready.Name, readiness}:       {},
			{notStartedNoReadiness.Name, startup}: {},
			{startedNoReadiness.Name, startup}:    {},
			{terminated.Name, readiness}:          {},
		},
	}
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, probedReady.ContainerID), results.Success, &v1.Pod{})
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, probedUnready.ContainerID), results.Failure, &v1.Pod{})
	m.startupManager.Set(kubecontainer.ParseContainerID(logger, startedNoReadiness.ContainerID), results.Success, &v1.Pod{})
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, terminated.ContainerID), results.Success, &v1.Pod{})

	m.UpdatePodStatus(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: testPodUID,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: unprobed.Name},
				{Name: probedReady.Name},
				{Name: probedPending.Name},
				{Name: probedUnready.Name},
				{Name: notStartedNoReadiness.Name},
				{Name: startedNoReadiness.Name},
				{Name: terminated.Name},
			},
		},
	}, &podStatus)

	expectedReadiness := map[containerProbeKey]bool{
		{unprobed.Name, readiness}:              true,
		{probedReady.Name, readiness}:           true,
		{probedPending.Name, readiness}:         false,
		{probedUnready.Name, readiness}:         false,
		{notStartedNoReadiness.Name, readiness}: false,
		{startedNoReadiness.Name, readiness}:    true,
		{terminated.Name, readiness}:            false,
	}
	for _, c := range podStatus.ContainerStatuses {
		expected, ok := expectedReadiness[containerProbeKey{c.Name, readiness}]
		if !ok {
			t.Fatalf("Missing expectation for test case: %v", c.Name)
		}
		if expected != c.Ready {
			t.Errorf("Unexpected readiness for container %v: Expected %v but got %v",
				c.Name, expected, c.Ready)
		}
	}
}

func TestUpdatePodStatusWithInitContainers(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	notStarted := v1.ContainerStatus{
		Name:        "not_started_container",
		ContainerID: "test://not_started_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	started := v1.ContainerStatus{
		Name:        "started_container",
		ContainerID: "test://started_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	terminated := v1.ContainerStatus{
		Name:        "terminated_container",
		ContainerID: "test://terminated_container_id",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{},
		},
	}

	m := newTestManager()
	// no cleanup: using fake workers.

	// Setup probe "workers" and cached results.
	m.workers = map[types.UID]map[containerProbeKey]*worker{
		testPodUID: {
			{notStarted.Name, startup}: {},
			{started.Name, startup}:    {},
		},
	}
	m.startupManager.Set(kubecontainer.ParseContainerID(logger, started.ContainerID), results.Success, &v1.Pod{})

	testCases := []struct {
		desc                        string
		expectedStartup             map[containerProbeKey]bool
		expectedReadiness           map[containerProbeKey]bool
		hasRestartableInitContainer bool
	}{
		{
			desc: "init containers",
			expectedStartup: map[containerProbeKey]bool{
				{notStarted.Name, startup}: false,
				{started.Name, startup}:    true,
				{terminated.Name, startup}: false,
			},
			expectedReadiness: map[containerProbeKey]bool{
				{notStarted.Name, readiness}: false,
				{started.Name, readiness}:    false,
				{terminated.Name, readiness}: true,
			},
			hasRestartableInitContainer: false,
		},
		{
			desc: "init container with Always restartPolicy",
			expectedStartup: map[containerProbeKey]bool{
				{notStarted.Name, startup}: false,
				{started.Name, startup}:    true,
				{terminated.Name, startup}: false,
			},
			expectedReadiness: map[containerProbeKey]bool{
				{notStarted.Name, readiness}: false,
				{started.Name, readiness}:    true,
				{terminated.Name, readiness}: false,
			},
			hasRestartableInitContainer: true,
		},
	}

	containerRestartPolicy := func(enableSidecarContainers bool) *v1.ContainerRestartPolicy {
		if !enableSidecarContainers {
			return nil
		}
		restartPolicy := v1.ContainerRestartPolicyAlways
		return &restartPolicy
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			podStatus := v1.PodStatus{
				Phase: v1.PodRunning,
				InitContainerStatuses: []v1.ContainerStatus{
					notStarted, started, terminated,
				},
			}

			m.UpdatePodStatus(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: testPodUID,
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name:          notStarted.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
						{
							Name:          started.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
						{
							Name:          terminated.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
					},
				},
			}, &podStatus)

			for _, c := range podStatus.InitContainerStatuses {
				{
					expected, ok := tc.expectedStartup[containerProbeKey{c.Name, startup}]
					if !ok {
						t.Fatalf("Missing expectation for test case: %v", c.Name)
					}
					if expected != *c.Started {
						t.Errorf("Unexpected startup for container %v: Expected %v but got %v",
							c.Name, expected, *c.Started)
					}
				}
				{
					expected, ok := tc.expectedReadiness[containerProbeKey{c.Name, readiness}]
					if !ok {
						t.Fatalf("Missing expectation for test case: %v", c.Name)
					}
					if expected != c.Ready {
						t.Errorf("Unexpected readiness for container %v: Expected %v but got %v",
							c.Name, expected, c.Ready)
					}
				}
			}
		})
	}
}

func (m *manager) extractedReadinessHandling(logger klog.Logger) {
	update := <-m.readinessManager.Updates()
	// This code corresponds to an extract from kubelet.syncLoopIteration()
	ready := update.Result == results.Success
	m.statusManager.SetContainerReadiness(logger, update.PodUID, update.ContainerID, ready)
}

func TestUpdateReadiness(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testPod := getTestPod()
	setTestProbe(testPod, readiness, v1.Probe{})
	m := newTestManager()
	defer cleanup(t, m)

	// Start syncing readiness without leaking goroutine.
	stopCh := make(chan struct{})
	go wait.Until(func() { m.extractedReadinessHandling(logger) }, 0, stopCh)
	defer func() {
		close(stopCh)
		// Send an update to exit extractedReadinessHandling()
		m.readinessManager.Set(kubecontainer.ContainerID{}, results.Success, &v1.Pod{})
	}()

	exec := syncExecProber{}
	exec.set(probe.Success, nil)
	m.prober.exec = &exec

	m.statusManager.SetPodStatus(logger, testPod, getTestRunningStatus())

	m.AddPod(ctx, testPod)
	probePaths := map[types.UID][]containerProbeKey{
		testPodUID: {{testContainerName, readiness}},
	}
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// Wait for ready status.
	if err := waitForReadyStatus(t, m, true); err != nil {
		t.Error(err)
	}

	// Prober fails.
	exec.set(probe.Failure, nil)

	// Wait for failed status.
	if err := waitForReadyStatus(t, m, false); err != nil {
		t.Error(err)
	}
}

func expectProbes(m *manager, expected map[types.UID][]containerProbeKey) error {
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()

	// Build a map of what we actually have
	actual := make(map[types.UID]sets.Set[containerProbeKey])
	for podUID, podWorkers := range m.workers {
		actual[podUID] = sets.New[containerProbeKey]()
		for cKey := range podWorkers {
			actual[podUID].Insert(cKey)
		}
	}

	// Compare
	var unexpected []string
	var missing []string

	// Check expected against actual
	for podUID, expectedKeys := range expected {
		actualKeys, ok := actual[podUID]
		if !ok {
			for _, ek := range expectedKeys {
				missing = append(missing, fmt.Sprintf("%s/%s/%s", podUID, ek.containerName, ek.probeType.String()))
			}
			continue
		}
		for _, ek := range expectedKeys {
			if !actualKeys.Has(ek) {
				missing = append(missing, fmt.Sprintf("%s/%s/%s", podUID, ek.containerName, ek.probeType.String()))
			} else {
				actualKeys.Delete(ek) // Remove so we can detect unexpected ones
			}
		}
		if actualKeys.Len() == 0 {
			delete(actual, podUID)
		}
	}

	// Anything left in actual is unexpected
	for podUID, actualKeys := range actual {
		for ak := range actualKeys {
			unexpected = append(unexpected, fmt.Sprintf("%s/%s/%s", podUID, ak.containerName, ak.probeType.String()))
		}
	}

	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}

	return fmt.Errorf("Unexpected probes: %v; Missing probes: %v", unexpected, missing)
}

const interval = 1 * time.Second

// Wait for the given workers to exit & clean up.
func waitForWorkerExit(t *testing.T, m *manager, expected map[types.UID][]containerProbeKey) error {
	for podUID, containerProbes := range expected {
		for _, cp := range containerProbes {
			condition := func() (bool, error) {
				_, exists := m.getWorker(podUID, cp.containerName, cp.probeType)
				return !exists, nil
			}
			if exited, _ := condition(); exited {
				continue // Already exited, no need to poll.
			}
			t.Logf("Polling worker for pod %s, container %s, probe %s", podUID, cp.containerName, cp.probeType.String())
			if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
				return err
			}
		}
	}

	return nil
}

// Wait for the given workers to exit & clean up.
func waitForReadyStatus(t *testing.T, m *manager, ready bool) error {
	condition := func() (bool, error) {
		status, ok := m.statusManager.GetPodStatus(testPodUID)
		if !ok {
			return false, fmt.Errorf("status not found: %q", testPodUID)
		}
		if len(status.ContainerStatuses) != 1 {
			return false, fmt.Errorf("expected single container, found %d", len(status.ContainerStatuses))
		}
		if status.ContainerStatuses[0].ContainerID != testContainerID.String() {
			return false, fmt.Errorf("expected container %q, found %q",
				testContainerID, status.ContainerStatuses[0].ContainerID)
		}
		return status.ContainerStatuses[0].Ready == ready, nil
	}
	t.Logf("Polling for ready state %v", ready)
	if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
		return err
	}

	return nil
}

// cleanup running probes to avoid leaking goroutines.
func cleanup(t *testing.T, m *manager) {
	m.CleanupPods(nil)

	condition := func() (bool, error) {
		workerCount := m.workerCount()
		if workerCount > 0 {
			t.Logf("Waiting for %d workers to exit...", workerCount)
		}
		return workerCount == 0, nil
	}
	if exited, _ := condition(); exited {
		return // Already exited, no need to poll.
	}
	if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
		t.Fatalf("Error during cleanup: %v", err)
	}
}

func TestAddPodDynamicContainers(t *testing.T) {
	ctx := ktesting.Init(t)
	m := newTestManager()
	defer cleanup(t, m)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "dynamic_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:           "container1",
					ReadinessProbe: defaultProbe,
				},
			},
		},
	}

	// 1. Initial AddPod with container1
	m.AddPod(ctx, &pod)
	if err := expectProbes(m, map[types.UID][]containerProbeKey{
		"dynamic_pod": {{"container1", readiness}},
	}); err != nil {
		t.Fatalf("Initial AddPod failed: %v", err)
	}

	// 2. AddPod again with an additional container (container2)
	pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
		Name:           "container2",
		ReadinessProbe: defaultProbe,
	})
	m.AddPod(ctx, &pod)
	if err := expectProbes(m, map[types.UID][]containerProbeKey{
		"dynamic_pod": {
			{"container1", readiness},
			{"container2", readiness},
		},
	}); err != nil {
		t.Fatalf("AddPod with additional container failed: %v", err)
	}

	// 3. AddPod again with container1 deleted
	pod.Spec.Containers = []v1.Container{
		{
			Name:           "container2",
			ReadinessProbe: defaultProbe,
		},
	}
	m.AddPod(ctx, &pod)

	// We need to wait for container1's worker to exit because stop is async
	if err := waitForWorkerExit(t, m, map[types.UID][]containerProbeKey{
		"dynamic_pod": {{"container1", readiness}},
	}); err != nil {
		t.Fatalf("Waiting for container1 worker to exit failed: %v", err)
	}

	if err := expectProbes(m, map[types.UID][]containerProbeKey{
		"dynamic_pod": {{"container2", readiness}},
	}); err != nil {
		t.Fatalf("AddPod with deleted container failed: %v", err)
	}
}
