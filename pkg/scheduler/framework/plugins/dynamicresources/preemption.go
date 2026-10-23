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

package dynamicresources

import (
	"context"
	"sync"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/utils/ptr"
)

var _ fwk.PreFilterExtensions = &DynamicResources{}

type nominatedPodInfo struct {
	pod               *v1.Pod
	nodeName          string
	allocationResults []resourceapi.AllocationResult
	victimClaims      sets.Set[types.UID]
}

// preemptionTracker caches per-nominated-pod simulated DRA allocations and tracks
// which ResourceClaims belong to victims of active nominations on each node.
// This avoids re-running the DRA allocator in Pass 1 of RunFilterPluginsWithNominatedPods
// and prevents lower-priority pods from stealing freed devices during the asynchronous
// victim termination and claim deallocation window.
type preemptionTracker struct {
	mu                 sync.RWMutex
	nominatedPods      map[types.UID]nominatedPodInfo
	podsByNode         map[string]sets.Set[types.UID]
	victimClaimsByNode map[string]sets.Set[types.UID]
	nodesByVictimClaim map[types.UID]string
	claimNamesByUID    map[types.UID]types.NamespacedName
}

func newPreemptionTracker() *preemptionTracker {
	return &preemptionTracker{
		nominatedPods:      make(map[types.UID]nominatedPodInfo),
		podsByNode:         make(map[string]sets.Set[types.UID]),
		victimClaimsByNode: make(map[string]sets.Set[types.UID]),
		nodesByVictimClaim: make(map[types.UID]string),
		claimNamesByUID:    make(map[types.UID]types.NamespacedName),
	}
}

func (t *preemptionTracker) addNominatedPod(pod *v1.Pod, nodeName string, allocs []resourceapi.AllocationResult, victimClaims map[types.UID]types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if prev, ok := t.nominatedPods[pod.UID]; ok && prev.nodeName != nodeName {
		if nodePods, exists := t.podsByNode[prev.nodeName]; exists {
			nodePods.Delete(pod.UID)
			if len(nodePods) == 0 {
				delete(t.podsByNode, prev.nodeName)
				t.clearNodeVictimClaimsLocked(prev.nodeName)
			}
		}
	}

	var copiedAllocs []resourceapi.AllocationResult
	if len(allocs) > 0 {
		copiedAllocs = make([]resourceapi.AllocationResult, len(allocs))
		for i := range allocs {
			copiedAllocs[i] = *allocs[i].DeepCopy()
		}
	}

	var copiedClaims sets.Set[types.UID]
	if len(victimClaims) > 0 {
		copiedClaims = sets.New[types.UID]()
		for claimUID := range victimClaims {
			copiedClaims.Insert(claimUID)
		}
	}

	t.nominatedPods[pod.UID] = nominatedPodInfo{
		pod:               pod,
		nodeName:          nodeName,
		allocationResults: copiedAllocs,
		victimClaims:      copiedClaims,
	}
	if t.podsByNode[nodeName] == nil {
		t.podsByNode[nodeName] = sets.New[types.UID]()
	}
	t.podsByNode[nodeName].Insert(pod.UID)

	if len(victimClaims) > 0 {
		if t.victimClaimsByNode[nodeName] == nil {
			t.victimClaimsByNode[nodeName] = sets.New[types.UID]()
		}
		for claimUID, nn := range victimClaims {
			t.victimClaimsByNode[nodeName].Insert(claimUID)
			t.nodesByVictimClaim[claimUID] = nodeName
			t.claimNamesByUID[claimUID] = nn
		}
	}
}

func (t *preemptionTracker) removeNominatedPod(podUID types.UID) (info nominatedPodInfo, remainingPodsOnNode int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.nominatedPods[podUID]
	if !ok {
		return nominatedPodInfo{}, 0
	}
	delete(t.nominatedPods, podUID)
	nodeName := info.nodeName
	if nodePods, exists := t.podsByNode[nodeName]; exists {
		nodePods.Delete(podUID)
		remainingPodsOnNode = len(nodePods)
		if remainingPodsOnNode == 0 {
			delete(t.podsByNode, nodeName)
		}
	}
	return info, remainingPodsOnNode
}

func (t *preemptionTracker) clearNodeVictimClaims(nodeName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clearNodeVictimClaimsLocked(nodeName)
}

func (t *preemptionTracker) clearNodeVictimClaimsLocked(nodeName string) {
	for claimUID := range t.victimClaimsByNode[nodeName] {
		delete(t.nodesByVictimClaim, claimUID)
		delete(t.claimNamesByUID, claimUID)
	}
	delete(t.victimClaimsByNode, nodeName)
}

func (t *preemptionTracker) getNominatedPodInfo(podUID types.UID) (nominatedPodInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.nominatedPods[podUID]
	return info, ok
}

func (t *preemptionTracker) getNominatedPodAllocations(podUID types.UID, nodeName string) []resourceapi.AllocationResult {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.nominatedPods[podUID]
	if !ok || info.nodeName != nodeName {
		return nil
	}
	return info.allocationResults
}

func (t *preemptionTracker) getNominatedPodsForNode(nodeName string) []nominatedPodInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	podUIDs := t.podsByNode[nodeName]
	if len(podUIDs) == 0 {
		return nil
	}
	res := make([]nominatedPodInfo, 0, len(podUIDs))
	for uid := range podUIDs {
		if info, ok := t.nominatedPods[uid]; ok {
			res = append(res, info)
		}
	}
	return res
}

func (t *preemptionTracker) getVictimClaimsForNode(nodeName string) sets.Set[types.UID] {
	t.mu.RLock()
	defer t.mu.RUnlock()

	claims := t.victimClaimsByNode[nodeName]
	if len(claims) == 0 {
		return nil
	}
	return claims.Clone()
}

func (t *preemptionTracker) getNodeForVictimClaim(claimUID types.UID) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodeName, ok := t.nodesByVictimClaim[claimUID]
	return nodeName, ok
}

func (t *preemptionTracker) isTrackedVictimClaim(claimUID types.UID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, ok := t.nodesByVictimClaim[claimUID]
	return ok
}

func (t *preemptionTracker) getVictimClaimNamespacedName(claimUID types.UID) (types.NamespacedName, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nn, ok := t.claimNamesByUID[claimUID]
	return nn, ok
}

func (t *preemptionTracker) removeVictimClaims(nodeName string, claimUIDs ...types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	nodeClaims := t.victimClaimsByNode[nodeName]
	for _, uid := range claimUIDs {
		delete(t.nodesByVictimClaim, uid)
		delete(t.claimNamesByUID, uid)
		if nodeClaims != nil {
			nodeClaims.Delete(uid)
		}
	}
	if nodeClaims != nil && len(nodeClaims) == 0 {
		delete(t.victimClaimsByNode, nodeName)
	}
}

func (pl *DynamicResources) getPreemptionTracker() *preemptionTracker {
	pl.preemptionTrackerOnce.Do(func() {
		if pl.preemptionTracker == nil {
			if mgr, ok := pl.draManager.(*DefaultDRAManager); ok && mgr.preemptionTracker != nil {
				pl.preemptionTracker = mgr.preemptionTracker
			} else {
				pl.preemptionTracker = newPreemptionTracker()
			}
		}
	})
	return pl.preemptionTracker
}

func (pl *DynamicResources) clearNominatedPodOnReserve(podUID types.UID) {
	tracker := pl.getPreemptionTracker()
	info, remaining := tracker.removeNominatedPod(podUID)
	if info.nodeName != "" {
		if remaining == 0 {
			tracker.clearNodeVictimClaims(info.nodeName)
		} else if len(info.victimClaims) > 0 {
			pl.pruneDeallocatedVictimClaims(info.nodeName)
		}
	}
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *DynamicResources) PreFilterExtensions() fwk.PreFilterExtensions {
	if !pl.fts.EnableDRAPreemption {
		return nil
	}
	return pl
}

// isPreemptionSupported reports whether DRA preemption simulation is supported for pod.
// In Alpha, pods belonging to a PodGroup and pods requesting extended resources backed by DRA
// are excluded from DRA preemption.
func (pl *DynamicResources) isPreemptionSupported(pod *v1.Pod, state *stateData) bool {
	if !pl.fts.EnableDRAPreemption || pod == nil {
		return false
	}
	if pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil {
		return false
	}
	if state != nil && state.claims.extendedResourceClaim() != nil {
		return false
	}
	return true
}

// RemovePod simulates removing a candidate victim pod's node-local DRA allocations from CycleState.
func (pl *DynamicResources) RemovePod(ctx context.Context, cs fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	state.sawRemovePodSinceClone = true
	if nodeInfo != nil && nodeInfo.Node() != nil {
		root := state.root()
		root.mutex.Lock()
		delete(root.pass1NodeAllocations, nodeInfo.Node().Name)
		root.mutex.Unlock()
	}

	if !pl.isPreemptionSupported(podToSchedule, state) || state.allocatedState == nil {
		return nil
	}
	if podInfoToRemove == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return nil
	}
	pod := podInfoToRemove.GetPod()
	if pod == nil || (pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil) {
		return nil
	}

	nodeName := nodeInfo.Node().Name
	if state.everRemovedUsers == nil {
		state.everRemovedUsers = sets.New[types.UID]()
	}
	state.everRemovedUsers.Insert(pod.UID)

	pl.foreachVictimResourceClaim(pod, func(claim *resourceapi.ResourceClaim, alloc *resourceapi.AllocationResult) {
		if !isAllocationLocalToNode(alloc, state.slices, nodeName) {
			return
		}
		if state.removedClaimUsers == nil {
			state.removedClaimUsers = make(map[types.UID]sets.Set[types.UID])
		}
		users, ok := state.removedClaimUsers[claim.UID]
		if !ok {
			users = sets.New[types.UID]()
			state.removedClaimUsers[claim.UID] = users
		}
		users.Insert(pod.UID)

		if state.removedVictimClaims.Has(claim.UID) {
			return
		}
		if !pl.allConsumersRemovedOrTerminating(claim, users, state.everRemovedUsers, nodeInfo) {
			return
		}

		removeAllocationFromState(alloc, state, pl.fts.EnableDRAConsumableCapacity)
		if state.removedVictimClaims == nil {
			state.removedVictimClaims = sets.New[types.UID]()
		}
		state.removedVictimClaims.Insert(claim.UID)
		state.allocatorDirty = true
	})

	return nil
}

// AddPod simulates adding back a reprieved victim pod's DRA allocations or a nominated pod's DRA allocations into CycleState.
func (pl *DynamicResources) AddPod(ctx context.Context, cs fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if !state.sawRemovePodSinceClone {
		state.isPass1Clone = true
	}
	if nodeInfo != nil && nodeInfo.Node() != nil {
		root := state.root()
		root.mutex.Lock()
		delete(root.pass1NodeAllocations, nodeInfo.Node().Name)
		root.mutex.Unlock()
	}

	if !pl.isPreemptionSupported(podToSchedule, state) || state.allocatedState == nil {
		return nil
	}
	if podInfoToAdd == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return nil
	}
	pod := podInfoToAdd.GetPod()
	if pod == nil || (pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil) {
		return nil
	}

	nodeName := nodeInfo.Node().Name
	if !state.everRemovedUsers.Has(pod.UID) {
		pl.foreachVictimResourceClaim(pod, func(claim *resourceapi.ResourceClaim, alloc *resourceapi.AllocationResult) {
			if !state.removedVictimClaims.Has(claim.UID) || !isAllocationLocalToNode(alloc, state.slices, nodeName) {
				return
			}
			addAllocationToState(alloc, state, pl.fts.EnableDRAConsumableCapacity)
			state.removedVictimClaims.Delete(claim.UID)
			state.allocatorDirty = true
		})
		if info, ok := pl.getPreemptionTracker().getNominatedPodInfo(pod.UID); ok && info.nodeName == nodeName {
			for claimUID := range info.victimClaims {
				if state.removedVictimClaims.Has(claimUID) {
					continue
				}
				claim, alloc := pl.getVictimClaimAndAllocation(claimUID)
				if claim == nil || alloc == nil || !isAllocationLocalToNode(alloc, state.slices, nodeName) {
					continue
				}
				reprieved := false
				for _, ref := range claim.Status.ReservedFor {
					if ref.Resource == "pods" && state.everRemovedUsers.Has(ref.UID) && !state.removedClaimUsers[claimUID].Has(ref.UID) {
						reprieved = true
						break
					}
				}
				if reprieved {
					continue
				}
				removeAllocationFromState(alloc, state, pl.fts.EnableDRAConsumableCapacity)
				if state.removedVictimClaims == nil {
					state.removedVictimClaims = sets.New[types.UID]()
				}
				state.removedVictimClaims.Insert(claimUID)
				state.allocatorDirty = true
			}
			for i := range info.allocationResults {
				addAllocationToState(&info.allocationResults[i], state, pl.fts.EnableDRAConsumableCapacity)
				state.allocatorDirty = true
			}
		}
		return nil
	}

	pl.foreachVictimResourceClaim(pod, func(claim *resourceapi.ResourceClaim, alloc *resourceapi.AllocationResult) {
		if !isAllocationLocalToNode(alloc, state.slices, nodeName) {
			return
		}
		users, ok := state.removedClaimUsers[claim.UID]
		if !ok || !users.Has(pod.UID) {
			return
		}
		users.Delete(pod.UID)

		if !state.removedVictimClaims.Has(claim.UID) {
			return
		}
		if pl.allConsumersRemovedOrTerminating(claim, users, state.everRemovedUsers, nodeInfo) {
			return
		}

		addAllocationToState(alloc, state, pl.fts.EnableDRAConsumableCapacity)
		state.removedVictimClaims.Delete(claim.UID)
		state.allocatorDirty = true
	})

	return nil
}

// foreachVictimResourceClaim invokes cb for each unique allocated or in-flight ResourceClaim
// referenced by pod in pod.Spec.ResourceClaims.
func (pl *DynamicResources) foreachVictimResourceClaim(pod *v1.Pod, cb func(claim *resourceapi.ResourceClaim, alloc *resourceapi.AllocationResult)) {
	var seen sets.Set[types.UID]
	for _, resource := range pod.Spec.ResourceClaims {
		claimName, mustCheckOwner, err := resourceclaim.Name(pod, &resource)
		if err != nil || claimName == nil {
			continue
		}
		claim, err := pl.draManager.ResourceClaims().Get(pod.Namespace, *claimName)
		if err != nil || claim == nil {
			continue
		}
		if seen.Has(claim.UID) {
			continue
		}
		if mustCheckOwner {
			if err := resourceclaim.IsForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims); err != nil {
				continue
			}
		}

		alloc := claim.Status.Allocation
		isPending := false
		if alloc == nil {
			alloc = pl.draManager.ResourceClaims().GetPendingAllocation(claim.UID)
			isPending = alloc != nil
		}
		if alloc == nil {
			continue
		}

		if len(claim.Status.ReservedFor) > 0 {
			if !resourceclaim.IsReservedForPod(pod, claim, false) {
				continue
			}
		} else if !isPending && resourceclaim.IsForPod(pod, claim, false) != nil {
			continue
		}

		if seen == nil {
			seen = sets.New[types.UID]()
		}
		seen.Insert(claim.UID)
		cb(claim, alloc)
	}
}

// isAllocationLocalToNode reports whether every device in alloc uses an explicit
// Spec.NodeName selector matching nodeName (either on the Device when PerDeviceNodeSelection
// is enabled, or on the ResourceSlice otherwise). Claims using NodeSelector or AllNodes
// are excluded from preemption simulation.
func isAllocationLocalToNode(alloc *resourceapi.AllocationResult, slices []*resourceapi.ResourceSlice, nodeName string) bool {
	if alloc == nil || len(alloc.Devices.Results) == 0 || nodeName == "" {
		return false
	}
	for _, res := range alloc.Devices.Results {
		found := false
		for _, slice := range slices {
			if slice.Spec.Driver != res.Driver || slice.Spec.Pool.Name != res.Pool {
				continue
			}
			for i := range slice.Spec.Devices {
				device := &slice.Spec.Devices[i]
				if device.Name != res.Device {
					continue
				}
				if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
					if device.NodeName == nil || *device.NodeName != nodeName {
						return false
					}
				} else {
					if slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
						return false
					}
				}
				found = true
				break
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// allConsumersRemovedOrTerminating reports whether every consumer in claim.Status.ReservedFor
// has either been removed in the simulation (present in removedUsers) or is a non-reprieved pod
// that is already terminating.
func (pl *DynamicResources) allConsumersRemovedOrTerminating(
	claim *resourceapi.ResourceClaim,
	removedUsers sets.Set[types.UID],
	everRemovedUsers sets.Set[types.UID],
	nodeInfo fwk.NodeInfo,
) bool {
	for _, ref := range claim.Status.ReservedFor {
		if ref.Resource != "pods" {
			return false
		}
		if removedUsers.Has(ref.UID) {
			continue
		}
		// If RemovePod was previously called for this consumer and AddPod reprieved it,
		// it is active in the current simulation even if its DeletionTimestamp is set.
		if everRemovedUsers.Has(ref.UID) {
			return false
		}
		if pl.isConsumerPodTerminating(claim.Namespace, ref.Name, ref.UID, nodeInfo) {
			continue
		}
		return false
	}
	return true
}

func (pl *DynamicResources) isConsumerPodTerminating(namespace, name string, uid types.UID, nodeInfo fwk.NodeInfo) bool {
	if nodeInfo != nil {
		for _, pi := range nodeInfo.GetPods() {
			p := pi.GetPod()
			if p != nil && p.UID == uid {
				return p.DeletionTimestamp != nil
			}
		}
	}
	if pl.podIndexer != nil {
		if obj, exists, err := pl.podIndexer.GetByKey(namespace + "/" + name); err == nil && exists {
			if p, ok := obj.(*v1.Pod); ok && p.UID == uid {
				return p.DeletionTimestamp != nil
			}
		}
	}
	return false
}

func removeAllocationFromState(alloc *resourceapi.AllocationResult, state *stateData, enableConsumableCapacity bool) {
	foreachAllocationResultDevice(alloc,
		func(deviceID structured.DeviceID) {
			state.allocatedState.AllocatedDevices.Delete(deviceID)
		},
		enableConsumableCapacity,
		func(sharedDeviceID structured.SharedDeviceID) {
			if state.sharedDeviceIDs != nil {
				state.sharedDeviceIDs.Delete(sharedDeviceID)
			}
			deviceID := sharedDeviceID.GetDeviceID()
			stillShared := false
			for existing := range state.sharedDeviceIDs {
				if existing.GetDeviceID() == deviceID {
					stillShared = true
					break
				}
			}
			if !stillShared {
				state.allocatedState.AllocatedSharedDeviceIDs.Delete(deviceID)
			}
		},
		func(capacity structured.DeviceConsumedCapacity) {
			state.allocatedState.AggregatedCapacity.Remove(capacity)
		},
	)
}

func addAllocationToState(alloc *resourceapi.AllocationResult, state *stateData, enableConsumableCapacity bool) {
	foreachAllocationResultDevice(alloc,
		func(deviceID structured.DeviceID) {
			state.allocatedState.AllocatedDevices.Insert(deviceID)
		},
		enableConsumableCapacity,
		func(sharedDeviceID structured.SharedDeviceID) {
			if state.sharedDeviceIDs == nil {
				state.sharedDeviceIDs = sets.New[structured.SharedDeviceID]()
			}
			state.sharedDeviceIDs.Insert(sharedDeviceID)
			state.allocatedState.AllocatedSharedDeviceIDs.Insert(sharedDeviceID.GetDeviceID())
		},
		func(capacity structured.DeviceConsumedCapacity) {
			state.allocatedState.AggregatedCapacity.Insert(capacity)
		},
	)
}

type poolKey struct {
	driver string
	pool   string
}

// validateAllocationResults verifies whether a previously computed set of AllocationResults
// (e.g., from Pass 1 of RunFilterPluginsWithNominatedPods) is valid against state.allocatedState
// on node.
func (pl *DynamicResources) validateAllocationResults(
	node *v1.Node,
	claimsToAllocate []*resourceapi.ResourceClaim,
	allocationResults []resourceapi.AllocationResult,
	state *stateData,
) bool {
	if len(allocationResults) != len(claimsToAllocate) || state.allocatedState == nil {
		return false
	}

	features := AllocatorFeatures(pl.fts)
	maxGen := make(map[poolKey]int64)
	for _, slice := range state.slices {
		pk := poolKey{driver: slice.Spec.Driver, pool: slice.Spec.Pool.Name}
		if gen, ok := maxGen[pk]; !ok || slice.Spec.Pool.Generation > gen {
			maxGen[pk] = slice.Spec.Pool.Generation
		}
	}

	newlyAllocatedCapacity := structured.NewConsumedCapacityCollection()
	newlyAllocatedDevices := sets.New[structured.DeviceID]()
	newlyConsumedCountersPools := sets.New[poolKey]()
	devicesConsumingCounters := sets.New[structured.DeviceID]()

	for _, alloc := range allocationResults {
		for _, res := range alloc.Devices.Results {
			pk := poolKey{driver: res.Driver, pool: res.Pool}
			var matchedSlice *resourceapi.ResourceSlice
			var matchedDevice *resourceapi.Device
			for _, slice := range state.slices {
				if slice.Spec.Driver != res.Driver || slice.Spec.Pool.Name != res.Pool || slice.Spec.Pool.Generation != maxGen[pk] {
					continue
				}
				for i := range slice.Spec.Devices {
					if slice.Spec.Devices[i].Name == res.Device {
						matchedSlice = slice
						matchedDevice = &slice.Spec.Devices[i]
						break
					}
				}
				if matchedDevice != nil {
					break
				}
			}
			if matchedSlice == nil || matchedDevice == nil {
				return false
			}

			if ptr.Deref(matchedSlice.Spec.PerDeviceNodeSelection, false) {
				matches, err := structured.NodeMatches(features, node, ptr.Deref(matchedDevice.NodeName, ""), ptr.Deref(matchedDevice.AllNodes, false), matchedDevice.NodeSelector)
				if err != nil || !matches {
					return false
				}
			} else {
				matches, err := structured.NodeMatches(features, node, ptr.Deref(matchedSlice.Spec.NodeName, ""), ptr.Deref(matchedSlice.Spec.AllNodes, false), matchedSlice.Spec.NodeSelector)
				if err != nil || !matches {
					return false
				}
			}

			if features.DeviceTaints {
				for _, taint := range matchedDevice.Taints {
					if taint.Effect != resourceapi.DeviceTaintEffectNoSchedule && taint.Effect != resourceapi.DeviceTaintEffectNoExecute {
						continue
					}
					tolerated := false
					for _, tol := range res.Tolerations {
						if resourceclaim.ToleratesTaint(tol, taint) {
							tolerated = true
							break
						}
					}
					if !tolerated {
						return false
					}
				}
			}

			if ptr.Deref(res.AdminAccess, false) {
				continue
			}

			deviceID := structured.MakeDeviceID(res.Driver, res.Pool, res.Device)
			allowMultiple := features.ConsumableCapacity && ptr.Deref(matchedDevice.AllowMultipleAllocations, false)
			alreadySharing := allowMultiple && (state.allocatedState.AllocatedSharedDeviceIDs.Has(deviceID) ||
				len(state.allocatedState.AggregatedCapacity[deviceID]) > 0 ||
				newlyAllocatedDevices.Has(deviceID))

			if res.ShareID != nil {
				if !allowMultiple {
					return false
				}
				if state.allocatedState.AllocatedDevices.Has(deviceID) {
					return false
				}
				if res.ConsumedCapacity != nil {
					devCap := structured.NewDeviceConsumedCapacity(deviceID, res.ConsumedCapacity)
					newlyAllocatedCapacity.Insert(devCap)
					for fqn, newQty := range newlyAllocatedCapacity[deviceID] {
						totalQty := newQty.DeepCopy()
						if existingCap, ok := state.allocatedState.AggregatedCapacity[deviceID]; ok {
							if existingQty, ok := existingCap[fqn]; ok && existingQty != nil {
								totalQty.Add(*existingQty)
							}
						}
						capVal, ok := draapi.LookupByQualifiedName(matchedDevice.Capacity, resourceapi.QualifiedName(fqn.String()), res.Driver)
						if !ok || capVal.Value.Cmp(totalQty) < 0 {
							return false
						}
					}
				}
				newlyAllocatedDevices.Insert(deviceID)
			} else {
				if state.allocatedState.AllocatedDevices.Has(deviceID) ||
					state.allocatedState.AllocatedSharedDeviceIDs.Has(deviceID) ||
					newlyAllocatedDevices.Has(deviceID) {
					return false
				}
				if existingCap, ok := state.allocatedState.AggregatedCapacity[deviceID]; ok && !existingCap.Empty() {
					return false
				}
				newlyAllocatedDevices.Insert(deviceID)
			}

			if len(matchedDevice.ConsumesCounters) > 0 {
				if !features.PartitionableDevices {
					return false
				}
				if !alreadySharing {
					devicesConsumingCounters.Insert(deviceID)
					newlyConsumedCountersPools.Insert(pk)
				}
			}
		}
	}

	for pk := range newlyConsumedCountersPools {
		availableCounters := make(map[string]map[string]resource.Quantity)
		for _, slice := range state.slices {
			if slice.Spec.Driver != pk.driver || slice.Spec.Pool.Name != pk.pool || slice.Spec.Pool.Generation != maxGen[pk] {
				continue
			}
			for _, cs := range slice.Spec.SharedCounters {
				if _, exists := availableCounters[cs.Name]; exists {
					continue
				}
				counters := make(map[string]resource.Quantity, len(cs.Counters))
				for name, c := range cs.Counters {
					counters[name] = c.Value.DeepCopy()
				}
				availableCounters[cs.Name] = counters
			}
		}

		for _, slice := range state.slices {
			if slice.Spec.Driver != pk.driver || slice.Spec.Pool.Name != pk.pool || slice.Spec.Pool.Generation != maxGen[pk] {
				continue
			}
			for _, dev := range slice.Spec.Devices {
				devID := structured.MakeDeviceID(pk.driver, pk.pool, dev.Name)
				if !structured.IsDeviceAllocated(devID, state.allocatedState) && !devicesConsumingCounters.Has(devID) {
					continue
				}
				for _, consumption := range dev.ConsumesCounters {
					csCounters, ok := availableCounters[consumption.CounterSet]
					if !ok {
						return false
					}
					for name, c := range consumption.Counters {
						avail, ok := csCounters[name]
						if !ok {
							return false
						}
						avail.Sub(c.Value)
						if avail.Sign() < 0 {
							return false
						}
						csCounters[name] = avail
					}
				}
			}
		}
	}

	return true
}

// AddNominatedPod caches the simulated DRA allocations for a newly nominated pod
// and records its victims' node-local ResourceClaims for settling-window protection.
func (pl *DynamicResources) AddNominatedPod(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string, victims []*v1.Pod) {
	if !pl.fts.EnableDRAPreemption || pod == nil || nodeName == "" {
		return
	}
	pl.pruneDeallocatedVictimClaims(nodeName)

	var state *stateData
	if cs != nil {
		state, _ = getStateData(cs)
	}

	var allocs []resourceapi.AllocationResult
	if state != nil {
		root := state.root()
		root.mutex.Lock()
		if na, ok := root.preemptionNodeAllocations[nodeName]; ok {
			allocs = na.allocationResults
		} else if na, ok := root.nodeAllocations[nodeName]; ok {
			allocs = na.allocationResults
		}
		root.mutex.Unlock()
	}

	var slices []*resourceapi.ResourceSlice
	if state != nil && len(state.slices) > 0 {
		slices = state.slices
	} else if len(victims) > 0 && pl.draManager != nil && pl.draManager.ResourceSlices() != nil {
		slices, _ = pl.draManager.ResourceSlices().ListWithDeviceTaintRules()
	}

	victimUIDs := sets.New[types.UID]()
	for _, v := range victims {
		if v != nil {
			victimUIDs.Insert(v.UID)
		}
	}

	victimClaims := make(map[types.UID]types.NamespacedName)
	for _, victim := range victims {
		if victim == nil || (victim.Spec.SchedulingGroup != nil && victim.Spec.SchedulingGroup.PodGroupName != nil) {
			continue
		}
		pl.foreachVictimResourceClaim(victim, func(claim *resourceapi.ResourceClaim, alloc *resourceapi.AllocationResult) {
			if len(slices) > 0 && !isAllocationLocalToNode(alloc, slices, nodeName) {
				return
			}
			if !pl.allConsumersRemovedOrTerminating(claim, victimUIDs, nil, nil) {
				return
			}
			victimClaims[claim.UID] = types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}
		})
	}

	if len(pod.Spec.ResourceClaims) == 0 && len(allocs) == 0 && len(victimClaims) == 0 {
		pl.clearNominatedPodOnReserve(pod.UID)
		return
	}

	pl.getPreemptionTracker().addNominatedPod(pod, nodeName, allocs, victimClaims)
}

// RemoveNominatedPod clears cached DRA preemption state for pod when its nomination is cleared.
func (pl *DynamicResources) RemoveNominatedPod(ctx context.Context, pod *v1.Pod) {
	if !pl.fts.EnableDRAPreemption || pod == nil {
		return
	}
	tracker := pl.getPreemptionTracker()
	info, remaining := tracker.removeNominatedPod(pod.UID)
	if info.nodeName != "" {
		pl.pruneDeallocatedVictimClaims(info.nodeName)
		if remaining == 0 && !pl.hasRemainingAllocatedVictimClaims(info.nodeName, "") {
			tracker.clearNodeVictimClaims(info.nodeName)
		}
	}
}

// PodEligibleToPreempt prevents a pod from preempting again while any victim ResourceClaim
// on its nominated node is still allocated or in-flight.
func (pl *DynamicResources) PodEligibleToPreempt(ctx context.Context, pod *v1.Pod, nominatedNodeName string) (bool, string) {
	if !pl.fts.EnableDRAPreemption || pod == nil {
		return true, ""
	}
	tracker := pl.getPreemptionTracker()

	if info, ok := tracker.getNominatedPodInfo(pod.UID); ok && len(info.victimClaims) > 0 {
		if pl.hasRemainingAllocatedClaims(info.victimClaims, "") {
			return false, "waiting for preempted ResourceClaims to be deallocated on nominated node"
		}
		tracker.removeVictimClaims(info.nodeName, info.victimClaims.UnsortedList()...)
		return true, ""
	}

	if nominatedNodeName != "" {
		nodeClaims := tracker.getVictimClaimsForNode(nominatedNodeName)
		if len(nodeClaims) > 0 {
			podPriority := corev1helpers.PodPriority(pod)
			anyAllocated := false
			for claimUID := range nodeClaims {
				if pl.getVictimClaimAllocation("", "", claimUID) == nil {
					continue
				}
				anyAllocated = true
				if pl.isClaimReservedForHigherOrEqualNominatedPod(claimUID, pod.UID, podPriority) {
					return false, "waiting for preempted ResourceClaims to be deallocated on nominated node"
				}
			}
			if !anyAllocated {
				tracker.clearNodeVictimClaims(nominatedNodeName)
			}
		}
	}

	return true, ""
}

func (pl *DynamicResources) getVictimClaimAndAllocation(claimUID types.UID) (*resourceapi.ResourceClaim, *resourceapi.AllocationResult) {
	if pl.draManager == nil || pl.draManager.ResourceClaims() == nil {
		return nil, nil
	}
	if nn, ok := pl.getPreemptionTracker().getVictimClaimNamespacedName(claimUID); ok && nn.Namespace != "" && nn.Name != "" {
		claim, err := pl.draManager.ResourceClaims().Get(nn.Namespace, nn.Name)
		if err != nil || claim == nil || claim.UID != claimUID {
			return nil, nil
		}
		if claim.Status.Allocation != nil {
			return claim, claim.Status.Allocation
		}
		if alloc := pl.draManager.ResourceClaims().GetPendingAllocation(claimUID); alloc != nil {
			return claim, alloc
		}
		return claim, nil
	}
	claims, _ := pl.draManager.ResourceClaims().List()
	for _, claim := range claims {
		if claim.UID != claimUID {
			continue
		}
		if claim.Status.Allocation != nil {
			return claim, claim.Status.Allocation
		}
		if alloc := pl.draManager.ResourceClaims().GetPendingAllocation(claimUID); alloc != nil {
			return claim, alloc
		}
		return claim, nil
	}
	return nil, nil
}

func (pl *DynamicResources) getVictimClaimAllocation(namespace, name string, claimUID types.UID) *resourceapi.AllocationResult {
	if pl.draManager == nil || pl.draManager.ResourceClaims() == nil {
		return nil
	}
	if namespace != "" && name != "" {
		claim, err := pl.draManager.ResourceClaims().Get(namespace, name)
		if err != nil || claim == nil || claim.UID != claimUID {
			return nil
		}
		if claim.Status.Allocation != nil {
			return claim.Status.Allocation
		}
		return pl.draManager.ResourceClaims().GetPendingAllocation(claimUID)
	}
	_, alloc := pl.getVictimClaimAndAllocation(claimUID)
	return alloc
}

func (pl *DynamicResources) hasRemainingAllocatedClaims(claimUIDs sets.Set[types.UID], excludeClaimUID types.UID) bool {
	for claimUID := range claimUIDs {
		if claimUID == excludeClaimUID {
			continue
		}
		if pl.getVictimClaimAllocation("", "", claimUID) != nil {
			return true
		}
	}
	return false
}

func (pl *DynamicResources) hasRemainingAllocatedVictimClaims(nodeName string, excludeClaimUID types.UID) bool {
	return pl.hasRemainingAllocatedClaims(pl.getPreemptionTracker().getVictimClaimsForNode(nodeName), excludeClaimUID)
}

func (pl *DynamicResources) pruneDeallocatedVictimClaims(nodeName string) {
	tracker := pl.getPreemptionTracker()
	var deallocated []types.UID
	for claimUID := range tracker.getVictimClaimsForNode(nodeName) {
		if pl.getVictimClaimAllocation("", "", claimUID) == nil {
			deallocated = append(deallocated, claimUID)
		}
	}
	if len(deallocated) > 0 {
		tracker.removeVictimClaims(nodeName, deallocated...)
	}
}

func (pl *DynamicResources) shouldSkipClaimDeallocationWakeup(pod *v1.Pod, deallocatedClaim *resourceapi.ResourceClaim) bool {
	if !pl.fts.EnableDRAPreemption || pod == nil || deallocatedClaim == nil {
		return false
	}
	tracker := pl.getPreemptionTracker()
	if info, ok := tracker.getNominatedPodInfo(pod.UID); ok && len(info.victimClaims) > 0 {
		if pl.hasRemainingAllocatedClaims(info.victimClaims, deallocatedClaim.UID) {
			return true
		}
		if info.victimClaims.Has(deallocatedClaim.UID) {
			return false
		}
	}

	victimNode, ok := tracker.getNodeForVictimClaim(deallocatedClaim.UID)
	if !ok {
		return false
	}

	nominatedForNode := tracker.getNominatedPodsForNode(victimNode)
	if pod.Status.NominatedNodeName == victimNode {
		if pl.hasRemainingAllocatedVictimClaims(victimNode, deallocatedClaim.UID) {
			return true
		}
		return false
	}

	if pl.isClaimReservedForHigherOrEqualNominatedPod(deallocatedClaim.UID, pod.UID, corev1helpers.PodPriority(pod)) {
		return true
	}

	if len(nominatedForNode) == 0 && !pl.hasRemainingAllocatedVictimClaims(victimNode, deallocatedClaim.UID) {
		tracker.removeVictimClaims(victimNode, deallocatedClaim.UID)
	}

	return false
}

func (pl *DynamicResources) isClaimReservedForHigherOrEqualNominatedPod(claimUID types.UID, candidateUID types.UID, candidatePriority int32) bool {
	tracker := pl.getPreemptionTracker()
	victimNode, ok := tracker.getNodeForVictimClaim(claimUID)
	if !ok {
		return false
	}
	nominatedForNode := tracker.getNominatedPodsForNode(victimNode)
	matchedSpecificOwner := false
	for _, np := range nominatedForNode {
		if !np.victimClaims.Has(claimUID) {
			continue
		}
		matchedSpecificOwner = true
		if np.pod != nil && np.pod.UID != candidateUID && corev1helpers.PodPriority(np.pod) >= candidatePriority {
			return true
		}
	}
	if matchedSpecificOwner {
		return false
	}
	for _, np := range nominatedForNode {
		if np.pod == nil || np.pod.UID == candidateUID {
			continue
		}
		if corev1helpers.PodPriority(np.pod) >= candidatePriority {
			return true
		}
	}
	return false
}

func (pl *DynamicResources) activateUnscheduledClaimPods(ctx context.Context, deallocatedClaim *resourceapi.ResourceClaim) {
	if pl.fh == nil || pl.fh.SharedInformerFactory() == nil || deallocatedClaim == nil {
		return
	}
	pods, err := pl.fh.SharedInformerFactory().Core().V1().Pods().Lister().List(labels.Everything())
	if err != nil {
		return
	}
	toActivate := make(map[string]*v1.Pod)
	for _, p := range pods {
		if p.Spec.NodeName != "" || p.DeletionTimestamp != nil {
			continue
		}
		hint, err := pl.isSchedulableAfterClaimChange(klog.FromContext(ctx), p, deallocatedClaim, nil)
		if err == nil && hint == fwk.Queue {
			toActivate[p.Namespace+"/"+p.Name] = p
		}
	}
	if len(toActivate) > 0 {
		pl.fh.Activate(klog.FromContext(ctx), toActivate)
	}
}
