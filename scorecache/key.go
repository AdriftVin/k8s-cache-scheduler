package scorecache

import (
	"hash/fnv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

// ScoreKey is the key of score caching and comparable (string/int64/uint64...)
type ScoreKey struct {
	// different scheduler profile / schedulerName using different cache
	SchedulerName string

	// common Score/Filter 
	PriorityClassName string
	RuntimeClassName  string

	// resource request(containers + pod overhead) 
	CPUMilli    int64
	MemoryBytes int64
	GPUCount    int64 //environment without GPU will be 0

	// hashing difficult fields
	NodeSelectorHash uint64
	AffinityHash     uint64
	TolerationsHash  uint64
	TopologyHash     uint64
}

// BuildScoreKey: construct score caching key from PodSpec
func BuildScoreKey(pod *v1.Pod) ScoreKey {
	var runtimeClass string
	if pod.Spec.RuntimeClassName != nil {
    	runtimeClass = *pod.Spec.RuntimeClassName
	}
	cpuMilli, memBytes, gpu := sumPodRequests(pod)

	return ScoreKey{
		SchedulerName:      schedulerNameOrDefault(pod.Spec.SchedulerName),
		PriorityClassName:  pod.Spec.PriorityClassName,
		RuntimeClassName:   runtimeClass,
		CPUMilli:           cpuMilli,
		MemoryBytes:        memBytes,
		GPUCount:           gpu,
		NodeSelectorHash:   deepHash(pod.Spec.NodeSelector),
		AffinityHash:       deepHash(pod.Spec.Affinity),
		TolerationsHash:    deepHash(pod.Spec.Tolerations),
		TopologyHash:       deepHash(pod.Spec.TopologySpreadConstraints),
	}
}

// struct entity is comparable
func Equal(a, b ScoreKey) bool {
	return a == b
}

func schedulerNameOrDefault(s string) string {
	if s == "" {
		return "default-scheduler"
	}
	return s
}

// deepHash: hashing any object
// note: NOT SEMANTIC EQUAL(eg. can be different for the order of inner affinity)
// but for score cache correctness is more important than hit rate
func deepHash(obj interface{}) uint64 {
	if obj == nil {
		return 0
	}
	h := fnv.New64a()
	hashutil.DeepHashObject(h, obj)
	return h.Sum64()
}

func sumPodRequests(pod *v1.Pod) (cpuMilli int64, memBytes int64, gpu int64) {
	var cpuQ resource.Quantity
	var memQ resource.Quantity

	// all container request
	for i := range pod.Spec.Containers {
		req := pod.Spec.Containers[i].Resources.Requests
		if v, ok := req[v1.ResourceCPU]; ok {
			cpuQ.Add(v)
		}
		if v, ok := req[v1.ResourceMemory]; ok {
			memQ.Add(v)
		}
		// eg.NVIDIA GPU
		if v, ok := req["nvidia.com/gpu"]; ok {
			gpu += v.Value()
		}
	}

	// Pod overhead can affect scoring/feasibility
	if pod.Spec.Overhead != nil {
		if v, ok := pod.Spec.Overhead[v1.ResourceCPU]; ok {
			cpuQ.Add(v)
		}
		if v, ok := pod.Spec.Overhead[v1.ResourceMemory]; ok {
			memQ.Add(v)
		}
	}

	return cpuQ.MilliValue(), memQ.Value(), gpu
}
