// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"encoding/json"

	"github.com/NVIDIA/cluster-readiness-engine/pkg/workload"

	corev1 "k8s.io/api/core/v1"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// Run:ai owns its native inference resources; Kubernetes keeps the shared definitions.
func applyInferenceScheduling(deps []nvcrev1alpha1.DependencySpec, gs *nvcrev1alpha1.GangSchedulerSpec) error {
	return applyRunaiInference(deps, gs.Queue)
}

// ApplyInferencePlacement constrains GPU servers with the same resolved target as the probe.
func ApplyInferencePlacement(deps []nvcrev1alpha1.DependencySpec, affinity *corev1.NodeAffinity, tolerations []corev1.Toleration, applyTolerations bool) error {
	return transformInferenceServers(deps, func(obj map[string]any, pods []map[string]any) error {
		if obj[keyKind] == kindRunaiInference || obj[keyKind] == kindRunaiDistributedInference {
			ps := &corev1.PodSpec{}
			if affinity != nil {
				ps.Affinity = &corev1.Affinity{NodeAffinity: affinity.DeepCopy()}
			}
			if applyTolerations {
				ps.Tolerations = tolerations
			}
			for _, nativeSpec := range pods {
				if err := workload.ApplyRunaiPlacement(nativeSpec, ps); err != nil {
					return err
				}
			}
			return nil
		}
		for _, pod := range pods {
			spec := ensureMap(pod, keySpec)
			if affinity != nil {
				ensureMap(spec, "affinity")["nodeAffinity"] = affinity.DeepCopy()
			}
			if applyTolerations {
				existing, _ := spec["tolerations"].([]any)
				for _, tolerance := range tolerations {
					existing = append(existing, tolerance)
				}
				spec["tolerations"] = existing
			}
		}
		return nil
	})
}

func transformInferenceServers(deps []nvcrev1alpha1.DependencySpec, transform func(map[string]any, []map[string]any) error) error {
	for i := range deps {
		var obj map[string]any
		if err := json.Unmarshal(deps[i].Raw, &obj); err != nil {
			return err
		}
		spec := ensureMap(obj, keySpec)
		var pods []map[string]any
		switch obj[keyKind] {
		case kindRunaiInference:
			pods = append(pods, spec)
		case kindRunaiDistributedInference:
			pods = append(pods, ensureMap(obj, "leaderSpec"), ensureMap(obj, "workerSpec"))
		case "Deployment":
			pods = append(pods, ensureMap(spec, keyTemplate))
		case "LeaderWorkerSet":
			group := ensureMap(spec, "leaderWorkerTemplate")
			pods = append(pods, ensureMap(group, "leaderTemplate"), ensureMap(group, "workerTemplate"))
		default:
			continue
		}
		if err := transform(obj, pods); err != nil {
			return err
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		deps[i].Raw = raw
	}
	return nil
}
