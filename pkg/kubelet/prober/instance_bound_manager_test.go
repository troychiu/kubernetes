/*
Copyright 2026 The Kubernetes Authors.

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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/probe"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// testProbeSpec never reaches a threshold, so workers created from it keep
// probing forever and publish nothing. Tests that care about a probe outcome
// override the threshold or the fake prober's answer.
var testProbeSpec = v1.Probe{FailureThreshold: 1000}

func newTestInstanceBoundManager(tCtx ktesting.TContext) *instanceBoundManager {
	m := NewInstanceBoundManager(
		tCtx,
		results.NewManager(),
		results.NewManager(),
		results.NewManager(),
		nil, // runner
		&record.FakeRecorder{},
	)
	// Don't actually execute probes. Failure keeps workers alive without
	// publishing anything, given testProbeSpec's threshold.
	m.prober.exec = &syncExecProber{fakeExecProber: fakeExecProber{result: probe.Failure}}
	return m
}

// testExecProber returns the fake probe handler installed by
// newTestInstanceBoundManager, whose answer can be changed while workers are
// running.
func testExecProber(m *instanceBoundManager) *syncExecProber {
	return m.prober.exec.(*syncExecProber)
}

// probedTestPod returns the standard test pod with the given probe types set on
// its container.
func probedTestPod(probeTypes ...probeType) *v1.Pod {
	pod := getTestPod()
	for _, probeType := range probeTypes {
		setTestProbe(pod, probeType, testProbeSpec)
	}
	return pod
}

func testID(id string) kubecontainer.ContainerID {
	return kubecontainer.ContainerID{Type: "test", ID: id}
}

func getInstanceBoundWorker(m *instanceBoundManager, podUID types.UID, containerName string, probeType probeType) (*instanceBoundWorker, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workers[probeKey{podUID, containerName, probeType}]
	return w, ok
}

// boundIDs describes which container instance each of a container's probe slots
// is bound to, for compact assertions.
func boundIDs(m *instanceBoundManager, pod *v1.Pod, containerName string) map[string]string {
	got := map[string]string{}
	for _, probeType := range allProbeTypes {
		if w, ok := getInstanceBoundWorker(m, pod.UID, containerName, probeType); ok {
			got[probeType.String()] = w.containerID.ID
		}
	}
	return got
}

func startTestProbes(tCtx ktesting.TContext, m *instanceBoundManager, pod *v1.Pod, containerID kubecontainer.ContainerID) {
	m.StartProbes(tCtx, pod, &pod.Spec.Containers[0], containerID, []string{"1.2.3.4"}, time.Now())
}

// TestInstanceBoundManagerStartProbesIsIdempotent covers race R2: the
// container-start hook and reconciliation can both reach StartProbes for the
// same container instance, and repeated syncs can reach it many times.
func TestInstanceBoundManagerStartProbesIsIdempotent(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(readiness, liveness)

		startTestProbes(tCtx, m, pod, testID("a"))
		first, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, readiness)

		for i := 0; i < 3; i++ {
			startTestProbes(tCtx, m, pod, testID("a"))
		}

		if got := m.workerCount(); got != 2 {
			tCtx.Errorf("worker count = %d, want 2 (one readiness, one liveness)", got)
		}
		if again, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, readiness); again != first {
			tCtx.Error("repeated StartProbes replaced the worker instead of leaving it alone")
		}
	})
}

// TestInstanceBoundManagerStartProbesReplacesStaleWorkers checks that a new
// container instance takes over its predecessor's slots, and that the results
// cached under the old instance's ID do not linger.
func TestInstanceBoundManagerStartProbesReplacesStaleWorkers(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(readiness, liveness)

		startTestProbes(tCtx, m, pod, testID("old"))
		old, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, readiness)

		startTestProbes(tCtx, m, pod, testID("new"))

		if old.ctx.Err() == nil {
			tCtx.Error("worker for the replaced container was not stopped")
		}
		want := map[string]string{"Readiness": "new", "Liveness": "new"}
		if got := boundIDs(m, pod, testContainerName); fmt.Sprint(got) != fmt.Sprint(want) {
			tCtx.Errorf("slots bound to %v, want %v", got, want)
		}
		if _, ok := m.readinessManager.Get(testID("old")); ok {
			tCtx.Error("results for the replaced container were left in the cache")
		}
		if _, ok := m.readinessManager.Get(testID("new")); !ok {
			tCtx.Error("no seeded result for the new container")
		}
	})
}

// TestInstanceBoundManagerLateStopIsIgnored covers race R1: a stop for a
// container that has already been replaced must not take down its successor.
func TestInstanceBoundManagerLateStopIsIgnored(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(*instanceBoundManager, kubecontainer.ContainerID)
	}{{
		name: "StopProbes",
		stop: (*instanceBoundManager).StopProbes,
	}, {
		name: "StopLivenessAndStartupProbes",
		stop: (*instanceBoundManager).StopLivenessAndStartupProbes,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
				m := newTestInstanceBoundManager(tCtx)
				defer m.CleanupPods(nil)
				pod := probedTestPod(readiness, liveness)

				startTestProbes(tCtx, m, pod, testID("old"))
				startTestProbes(tCtx, m, pod, testID("new"))
				current, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness)

				tc.stop(m, testID("old"))

				if got, ok := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness); !ok || got != current {
					tCtx.Error("a stop for the previous container instance took down the current one")
				}
				if current.ctx.Err() != nil {
					tCtx.Error("current worker's probes were cancelled by a stop for the previous instance")
				}
			})
		})
	}
}

// TestInstanceBoundManagerStopLivenessAndStartupProbes checks that liveness and
// startup stop while readiness keeps running, so that a container being
// gracefully terminated is taken out of service rather than killed again.
func TestInstanceBoundManagerStopLivenessAndStartupProbes(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(readiness, liveness)

		startTestProbes(tCtx, m, pod, testID("a"))
		m.StopLivenessAndStartupProbes(testID("a"))

		want := map[string]string{"Readiness": "a"}
		if got := boundIDs(m, pod, testContainerName); fmt.Sprint(got) != fmt.Sprint(want) {
			tCtx.Errorf("slots bound to %v, want %v", got, want)
		}
		// Results survive: the container is still running, and its last known
		// state is still what the pod status should report.
		if _, ok := m.livenessManager.Get(testID("a")); !ok {
			tCtx.Error("liveness result was dropped before the container stopped")
		}
	})
}

// TestInstanceBoundManagerStopProbes checks the post-stop teardown: every
// worker for the instance goes away, along with everything cached about it.
func TestInstanceBoundManagerStopProbes(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(readiness, liveness)

		startTestProbes(tCtx, m, pod, testID("a"))
		m.StopProbes(testID("a"))

		if got := m.workerCount(); got != 0 {
			tCtx.Errorf("worker count = %d after StopProbes, want 0", got)
		}
		for name, rm := range map[string]results.Manager{"readiness": m.readinessManager, "liveness": m.livenessManager, "startup": m.startupManager} {
			if _, ok := rm.Get(testID("a")); ok {
				tCtx.Errorf("%s result survived StopProbes", name)
			}
		}
	})
}

// TestInstanceBoundManagerStopCancelsInFlightProbe covers race R4: a probe that
// is already executing when the container starts being killed is aborted rather
// than left running inside a container that is going away.
func TestInstanceBoundManagerStopCancelsInFlightProbe(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		runner := &hangingRunner{entered: make(chan struct{})}
		m.prober = newProber(runner, &record.FakeRecorder{})
		pod := probedTestPod(liveness)

		startTestProbes(tCtx, m, pod, testID("a"))
		w, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness)

		<-runner.entered
		m.StopLivenessAndStartupProbes(testID("a"))

		if w.ctx.Err() == nil {
			tCtx.Error("in-flight probe was not cancelled")
		}
		tCtx.Wait()
		if _, ok := m.livenessManager.Get(testID("a")); !ok {
			tCtx.Error("seeded liveness result disappeared")
		} else if result, _ := m.livenessManager.Get(testID("a")); result != results.Success {
			tCtx.Errorf("cancelled probe recorded %v; it should have been discarded", result)
		}
	})
}

// TestInstanceBoundManagerRemoveWorkerComparesIdentity covers race R5: a worker
// that exits just as its slot is refilled must not delete its replacement.
func TestInstanceBoundManagerRemoveWorkerComparesIdentity(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(liveness)

		startTestProbes(tCtx, m, pod, testID("old"))
		old, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness)
		startTestProbes(tCtx, m, pod, testID("new"))
		replacement, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness)

		// The stopped worker's goroutine only now gets around to cleaning up.
		m.removeWorker(old)

		if got, ok := getInstanceBoundWorker(m, pod.UID, testContainerName, liveness); !ok || got != replacement {
			tCtx.Error("an exiting worker deleted the slot belonging to its replacement")
		}
	})
}

// TestInstanceBoundManagerRestartStorm covers race R6: many containers of a pod
// restarting at once is just N independent replacements, with no special
// handling in the prober.
func TestInstanceBoundManagerRestartStorm(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)

		pod := getTestPod()
		pod.Spec.Containers = nil
		names := []string{"c0", "c1", "c2"}
		for _, name := range names {
			container := v1.Container{Name: name, LivenessProbe: testProbeSpec.DeepCopy()}
			container.LivenessProbe.ProbeHandler = v1.ProbeHandler{Exec: &v1.ExecAction{}}
			container.LivenessProbe.PeriodSeconds = 1
			container.LivenessProbe.TimeoutSeconds = 1
			container.LivenessProbe.SuccessThreshold = 1
			pod.Spec.Containers = append(pod.Spec.Containers, container)
		}

		start := func(generation string) {
			for i := range pod.Spec.Containers {
				c := &pod.Spec.Containers[i]
				m.StartProbes(tCtx, pod, c, testID(c.Name+"-"+generation), []string{"1.2.3.4"}, time.Now())
			}
		}

		start("v1")
		start("v2")

		if got := m.workerCount(); got != len(names) {
			tCtx.Errorf("worker count = %d, want %d", got, len(names))
		}
		for _, name := range names {
			want := map[string]string{"Liveness": name + "-v2"}
			if got := boundIDs(m, pod, name); fmt.Sprint(got) != fmt.Sprint(want) {
				tCtx.Errorf("container %s bound to %v, want %v", name, got, want)
			}
			if _, ok := m.livenessManager.Get(testID(name + "-v1")); ok {
				tCtx.Errorf("container %s left results cached under its previous instance", name)
			}
		}
	})
}

// TestInstanceBoundManagerStartupGate checks that readiness and liveness only
// begin once the startup probe has passed, and that they start immediately when
// it does rather than waiting for the next sync.
func TestInstanceBoundManagerStartupGate(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(startup, readiness, liveness)

		startTestProbes(tCtx, m, pod, testID("a"))

		want := map[string]string{"Startup": "a"}
		if got := boundIDs(m, pod, testContainerName); fmt.Sprint(got) != fmt.Sprint(want) {
			tCtx.Fatalf("before startup succeeded, slots bound to %v, want %v", got, want)
		}

		// Let the startup probe pass.
		testExecProber(m).set(probe.Success, nil)
		time.Sleep(2 * time.Second)
		tCtx.Wait()

		want = map[string]string{"Readiness": "a", "Liveness": "a"}
		if got := boundIDs(m, pod, testContainerName); fmt.Sprint(got) != fmt.Sprint(want) {
			tCtx.Errorf("after startup succeeded, slots bound to %v, want %v", got, want)
		}
		// The startup result outlives its worker: the runtime still reads it to
		// decide the container has started.
		if result, ok := m.startupManager.Get(testID("a")); !ok || result != results.Success {
			tCtx.Errorf("startup result = (%v, %v), want (Success, true)", result, ok)
		}
	})
}

// TestInstanceBoundManagerStartProbesSkipsStartupWhenAlreadyStarted checks the
// other half of the gate: a container already known to have started gets its
// readiness and liveness workers directly. This is the kubelet-restart case,
// where the startup result is reconstructed before probes are started.
func TestInstanceBoundManagerStartProbesSkipsStartupWhenAlreadyStarted(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		m := newTestInstanceBoundManager(tCtx)
		defer m.CleanupPods(nil)
		pod := probedTestPod(startup, readiness, liveness)

		m.startupManager.Seed(testID("a"), results.Success)
		startTestProbes(tCtx, m, pod, testID("a"))

		want := map[string]string{"Readiness": "a", "Liveness": "a"}
		if got := boundIDs(m, pod, testContainerName); fmt.Sprint(got) != fmt.Sprint(want) {
			tCtx.Errorf("slots bound to %v, want %v", got, want)
		}
	})
}

// TestInstanceBoundManagerStartupCallbackAfterKill covers race R8: a startup
// probe that succeeds just as its container is being killed must not bring up
// probes for a container that is on its way out.
func TestInstanceBoundManagerStartupCallbackAfterKill(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(*instanceBoundManager, *v1.Pod, ktesting.TContext)
	}{{
		name: "container killed",
		after: func(m *instanceBoundManager, pod *v1.Pod, tCtx ktesting.TContext) {
			m.StopProbes(testID("a"))
		},
	}, {
		name: "container replaced",
		after: func(m *instanceBoundManager, pod *v1.Pod, tCtx ktesting.TContext) {
			startTestProbes(tCtx, m, pod, testID("b"))
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
				m := newTestInstanceBoundManager(tCtx)
				defer m.CleanupPods(nil)
				pod := probedTestPod(startup, readiness, liveness)

				startTestProbes(tCtx, m, pod, testID("a"))
				w, _ := getInstanceBoundWorker(m, pod.UID, testContainerName, startup)

				tc.after(m, pod, tCtx)

				// The startup worker's probe was already in flight and only now
				// reports success.
				m.onStartupSucceeded(w)

				for _, probeType := range []probeType{readiness, liveness} {
					if got, ok := getInstanceBoundWorker(m, pod.UID, testContainerName, probeType); ok && got.containerID == testID("a") {
						tCtx.Errorf("%v worker was created for a container that is gone", probeType)
					}
				}
			})
		})
	}
}

// TestInstanceBoundManagerPodTeardown checks the two pod-scoped exits:
// RemovePod for a pod the sync loop knows about, and CleanupPods for one it
// does not.
func TestInstanceBoundManagerPodTeardown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		teardown func(*instanceBoundManager, *v1.Pod)
	}{{
		name:     "RemovePod",
		teardown: (*instanceBoundManager).RemovePod,
	}, {
		name: "CleanupPods",
		teardown: func(m *instanceBoundManager, pod *v1.Pod) {
			m.CleanupPods(map[types.UID]sets.Empty{"some-other-pod": {}})
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
				m := newTestInstanceBoundManager(tCtx)
				defer m.CleanupPods(nil)
				pod := probedTestPod(readiness, liveness)

				startTestProbes(tCtx, m, pod, testID("a"))
				tc.teardown(m, pod)

				if got := m.workerCount(); got != 0 {
					tCtx.Errorf("worker count = %d after teardown, want 0", got)
				}
				if _, ok := m.readinessManager.Get(testID("a")); ok {
					tCtx.Error("results survived pod teardown")
				}
			})
		})
	}
}
