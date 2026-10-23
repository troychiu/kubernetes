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

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/utils/ptr"
)

var _ fwk.PreFilterExtensions = &DynamicResources{}

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
