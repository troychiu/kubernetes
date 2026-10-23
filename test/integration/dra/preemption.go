/*
Copyright The Kubernetes Authors.

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

package dra

import (
	"fmt"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/ktesting"
	"k8s.io/client-go/util/retry"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/utils/ptr"
)

func createPreemptionTestNode(tCtx ktesting.TContext, namespace string) string {
	tCtx.Helper()
	nodeName := "node-" + namespace
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				"kubernetes.io/hostname": nodeName,
			},
		},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    "dra-preemption-test",
					Value:  namespace,
					Effect: v1.TaintEffectNoSchedule,
				},
			},
		},
	}
	node, err := tCtx.Client().CoreV1().Nodes().Create(tCtx, node, metav1.CreateOptions{FieldValidation: "Strict"})
	tCtx.ExpectNoError(err, "create dedicated preemption node")

	node.Status = v1.NodeStatus{
		Capacity: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(nodeCPUCapacity),
			v1.ResourceMemory: resource.MustParse(nodeMemoryCapacity),
			v1.ResourcePods:   *resource.NewScaledQuantity(maxPodsPerNode, 0),
		},
		Phase: v1.NodeRunning,
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionTrue,
			},
		},
	}
	node, err = tCtx.Client().CoreV1().Nodes().UpdateStatus(tCtx, node, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "update dedicated preemption node status")

	node.Spec.Taints = []v1.Taint{
		{
			Key:    "dra-preemption-test",
			Value:  namespace,
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	_, err = tCtx.Client().CoreV1().Nodes().Update(tCtx, node, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "remove not-ready taint from dedicated preemption node")

	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		_ = tCtx.Client().CoreV1().Nodes().Delete(tCtx, nodeName, metav1.DeleteOptions{})
	})
	return nodeName
}

func createPreemptionPriorityClasses(tCtx ktesting.TContext, prefix string) (low, medium, high string) {
	tCtx.Helper()
	createPC := func(suffix string, val int32) string {
		name := prefix + "-pc-" + suffix
		pc := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Value:      val,
		}
		_, err := tCtx.Client().SchedulingV1().PriorityClasses().Create(tCtx, pc, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create PriorityClass "+name)
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().SchedulingV1().PriorityClasses().Delete(tCtx, name, metav1.DeleteOptions{})
		})
		return name
	}
	return createPC("low", 10), createPC("medium", 50), createPC("high", 100)
}

func makePreemptionPod(namespace, name, priorityClassName string, gracePeriod *int64, claims ...*resourceapi.ResourceClaim) *v1.Pod {
	p := st.MakePod().Name(name).Namespace(namespace).
		Container("my-container").
		ZeroTerminationGracePeriod().
		Toleration("dra-preemption-test").
		Obj()
	p.Spec.PriorityClassName = priorityClassName
	p.Spec.Tolerations = []v1.Toleration{
		{
			Key:      "dra-preemption-test",
			Operator: v1.TolerationOpEqual,
			Value:    namespace,
			Effect:   v1.TaintEffectNoSchedule,
		},
	}
	p.Spec.TerminationGracePeriodSeconds = gracePeriod
	for _, c := range claims {
		p.Spec.ResourceClaims = append(p.Spec.ResourceClaims, v1.PodResourceClaim{
			Name:              c.Name,
			ResourceClaimName: &c.Name,
		})
	}
	return p
}

func testPreemption(tCtx ktesting.TContext) {
	tCtx.Parallel()
	lowPC, mediumPC, highPC := createPreemptionPriorityClasses(tCtx, "dra-preempt")

	tCtx.Run("BasicExclusiveDevice", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := st.MakeResourceSlice(nodeName, driverName).Devices("dev-0").Obj()
		createSlice(tCtx, slice)

		victimClaim := createClaim(tCtx, namespace, "-victim", class, claim)
		victimPod := makePreemptionPod(namespace, "victim", lowPC, ptr.To(int64(0)), victimClaim)
		createPod(tCtx, namespace, "", victimPod)
		waitForPodScheduled(tCtx, namespace, victimPod.Name)

		preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)
	})

	tCtx.Run("SharedClaimMultiVictim", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := st.MakeResourceSlice(nodeName, driverName).Devices("dev-0").Obj()
		createSlice(tCtx, slice)

		sharedClaim := createClaim(tCtx, namespace, "-shared", class, claim)
		victim1 := makePreemptionPod(namespace, "victim-1", lowPC, ptr.To(int64(0)), sharedClaim)
		victim2 := makePreemptionPod(namespace, "victim-2", mediumPC, ptr.To(int64(0)), sharedClaim)
		createPod(tCtx, namespace, "", victim1)
		createPod(tCtx, namespace, "", victim2)
		waitForPodScheduled(tCtx, namespace, victim1.Name)
		waitForPodScheduled(tCtx, namespace, victim2.Name)

		preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)
	})

	tCtx.Run("ReprieveHigherPriorityVictim", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := st.MakeResourceSlice(nodeName, driverName).Devices("dev-0", "dev-1").Obj()
		createSlice(tCtx, slice)

		victim1Claim := createClaim(tCtx, namespace, "-victim-1", class, claim)
		victim2Claim := createClaim(tCtx, namespace, "-victim-2", class, claim)
		victim1 := makePreemptionPod(namespace, "victim-1", lowPC, ptr.To(int64(0)), victim1Claim)
		victim2 := makePreemptionPod(namespace, "victim-2", mediumPC, ptr.To(int64(0)), victim2Claim)
		createPod(tCtx, namespace, "", victim1)
		createPod(tCtx, namespace, "", victim2)
		waitForPodScheduled(tCtx, namespace, victim1.Name)
		waitForPodScheduled(tCtx, namespace, victim2.Name)

		preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)

		// Verify victim-2 (higher priority victim) was reprieved and still exists.
		v2, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victim2.Name, metav1.GetOptions{})
		require.NoError(tCtx, err)
		assert.Nil(tCtx, v2.DeletionTimestamp)
	})

	tCtx.Run("ConsumableCapacityPartialPreemption", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace + "-slice",
			},
			Spec: resourceapi.ResourceSliceSpec{
				Driver:   driverName,
				NodeName: &nodeName,
				Pool: resourceapi.ResourcePool{
					Name:               nodeName,
					ResourceSliceCount: 1,
				},
				Devices: []resourceapi.Device{
					{
						Name:                     "shareable-dev",
						AllowMultipleAllocations: ptr.To(true),
						Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
							"memory": {Value: resource.MustParse("16Gi")},
						},
					},
				},
			},
		}
		createSlice(tCtx, slice)

		makeCapClaim := func(suffix string) *resourceapi.ResourceClaim {
			c := claim.DeepCopy()
			c.Spec.Devices.Requests[0].Exactly.Capacity = &resourceapi.CapacityRequirements{
				Requests: map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("8Gi"),
				},
			}
			return createClaim(tCtx, namespace, suffix, class, c)
		}

		v1Claim := makeCapClaim("-v1")
		v2Claim := makeCapClaim("-v2")
		victim1 := makePreemptionPod(namespace, "victim-1", lowPC, ptr.To(int64(0)), v1Claim)
		victim2 := makePreemptionPod(namespace, "victim-2", mediumPC, ptr.To(int64(0)), v2Claim)
		createPod(tCtx, namespace, "", victim1)
		createPod(tCtx, namespace, "", victim2)
		waitForPodScheduled(tCtx, namespace, victim1.Name)
		waitForPodScheduled(tCtx, namespace, victim2.Name)

		preemptorClaim := makeCapClaim("-preemptor")
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)

		// Verify victim-2 was reprieved and still shares the device.
		v2, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victim2.Name, metav1.GetOptions{})
		require.NoError(tCtx, err)
		assert.Nil(tCtx, v2.DeletionTimestamp)
	})

	tCtx.Run("PreBindVictim", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace + "-slice",
			},
			Spec: resourceapi.ResourceSliceSpec{
				Driver:   driverName,
				NodeName: &nodeName,
				Pool: resourceapi.ResourcePool{
					Name:               nodeName,
					ResourceSliceCount: 1,
				},
				Devices: []resourceapi.Device{
					{
						Name:                     "dev-binding",
						BindingConditions:        []string{bindingCondition},
						BindingFailureConditions: []string{failureCondition},
					},
				},
			},
		}
		createSlice(tCtx, slice)

		victimClaim := createClaim(tCtx, namespace, "-victim", class, claim)
		victimPod := makePreemptionPod(namespace, "victim", lowPC, ptr.To(int64(0)), victimClaim)
		createPod(tCtx, namespace, "", victimPod)

		// Wait until victimClaim is allocated in PreBind and victimPod is waiting on BindingConditions.
		waitForClaimAllocatedToDevice(tCtx, namespace, victimClaim.Name, schedulingTimeout)

		// Create high-priority preemptor while victimPod is blocked in PreBind.
		preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		// Unreserve on the PreBind victim deallocates victimClaim, allowing preemptorClaim to be allocated in PreBind.
		waitForClaimAllocatedToDevice(tCtx, namespace, preemptorClaim.Name, schedulingTimeout)

		// Satisfy the binding condition for preemptorClaim so preemptorPod completes binding.
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, preemptorClaim.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			latest.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
				Driver: driverName,
				Pool:   nodeName,
				Device: "dev-binding",
				Conditions: []metav1.Condition{{
					Type:               bindingCondition,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: latest.Generation,
					LastTransitionTime: metav1.Now(),
					Reason:             "Testing",
					Message:            "Ready",
				}},
			}}
			_, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, latest, metav1.UpdateOptions{})
			return err
		})
		tCtx.ExpectNoError(err, "satisfy binding condition for preemptorClaim")

		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)
	})

	tCtx.Run("SettlingWindowProtection", func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		namespace := createTestNamespace(tCtx, nil)
		nodeName := createPreemptionTestNode(tCtx, namespace)
		class, driverName := createTestClass(tCtx, namespace)
		startScheduler(tCtx)
		startClaimController(tCtx)

		slice := st.MakeResourceSlice(nodeName, driverName).Devices("dev-0").Obj()
		createSlice(tCtx, slice)

		// Give victim a non-zero grace period (30s) so it remains terminating until we force-delete it.
		victimClaim := createClaim(tCtx, namespace, "-victim", class, claim)
		victimPod := makePreemptionPod(namespace, "victim", lowPC, ptr.To(int64(30)), victimClaim)
		createPod(tCtx, namespace, "", victimPod)
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().CoreV1().Pods(namespace).Delete(tCtx, victimPod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
		})
		waitForPodScheduled(tCtx, namespace, victimPod.Name)

		preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
		preemptorPod := makePreemptionPod(namespace, "preemptor", highPC, ptr.To(int64(0)), preemptorClaim)
		createPod(tCtx, namespace, "", preemptorPod)

		// Wait for t1: preemptor is nominated and victim has DeletionTimestamp set.
		tCtx.Eventually(func(tCtx ktesting.TContext) (string, error) {
			p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, preemptorPod.Name, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			return p.Status.NominatedNodeName, nil
		}).WithTimeout(schedulingTimeout).Should(gomega.Equal(nodeName))

		tCtx.Eventually(func(tCtx ktesting.TContext) (*metav1.Time, error) {
			v, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victimPod.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return v.DeletionTimestamp, nil
		}).WithTimeout(schedulingTimeout).ShouldNot(gomega.BeNil())

		// Create medium-priority pod (priority 50, between victim 10 and preemptor 100) during settling window.
		mediumClaim := createClaim(tCtx, namespace, "-medium", class, claim)
		mediumPod := makePreemptionPod(namespace, "medium", mediumPC, ptr.To(int64(0)), mediumClaim)
		createPod(tCtx, namespace, "", mediumPod)

		// Give mediumPod a brief moment to attempt scheduling while victim is terminating, then force-delete victim (t2).
		time.Sleep(100 * time.Millisecond)
		err := tCtx.Client().CoreV1().Pods(namespace).Delete(tCtx, victimPod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
		if !apierrors.IsNotFound(err) {
			tCtx.ExpectNoError(err, "force delete victim pod")
		}

		// Preemptor (priority 100) must get scheduled on nodeName, not mediumPod (priority 50).
		scheduled := waitForPodScheduled(tCtx, namespace, preemptorPod.Name)
		assert.Equal(tCtx, nodeName, scheduled.Spec.NodeName)

		med, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, mediumPod.Name, metav1.GetOptions{})
		require.NoError(tCtx, err)
		assert.Empty(tCtx, med.Spec.NodeName, fmt.Sprintf("mediumPod should not have stolen the device from nominated preemptor: %+v", med.Spec))
	})
}
