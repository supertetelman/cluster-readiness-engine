// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/catalog"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/platform"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// Servers must retire with the probe so the next category can use their GPUs.
func TestVLLMDependencyLifecycle(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "vllm-dependency-lifecycle"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Variant string `json:"variant"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		entry := catalog.Lookup("inference", input.Variant)
		require.NotNil(tc.T, entry)
		spec, err := entry.Build(nvcrev1alpha1.TargetSpec{}, catalog.BuildConfig{NodesPerJob: 1, GpusPerNode: 1, GPUArchitecture: "a100"})
		if err != nil {
			return err
		}
		if err := platform.ApplySchedulingBackend(&spec, &nvcrev1alpha1.SchedulingBackend{Name: "runai", Queue: "nvcre"}); err != nil {
			return err
		}
		original, err := json.Marshal(spec.Dependencies)
		if err != nil {
			return err
		}
		scoped := spec.DeepCopy()
		target := &nvcrev1alpha1.TargetSpec{NodeSelector: map[string]string{"gpu-family": "a100"}, NodeNames: []string{"gpu-a", "gpu-b"}}
		affinity := BuildNodeAffinity(PinnedHostnames(nvcrev1alpha1.PlacementUnpinned, nil, target), target, []string{"NVIDIA-A100-PCIE-40GB"})
		if err := platform.ApplyInferencePlacement(scoped.Dependencies, affinity, []corev1.Toleration{{Key: gpuPresentTaintKey, Operator: corev1.TolerationOpExists}}, true); err != nil {
			return err
		}
		after, err := json.Marshal(spec.Dependencies)
		if err != nil {
			return err
		}
		require.JSONEq(tc.T, string(original), string(after), "per-job placement must not mutate the shared Workflow templates")
		spec = *scoped
		raw, err := marshalJobSpecForDependencyClassification(&spec.JobTemplate.Spec)
		if err != nil {
			return err
		}
		workflowDeps, jobDeps := classifyDependencies(spec.Dependencies, raw)
		var workflowNames []string
		for _, dep := range workflowDeps {
			workflowNames = append(workflowNames, extractMetadataName(dep.Raw))
		}
		replacements := buildReplacementMap(jobDeps, "-validation")
		patched, err := suffixJobSpec(&spec.JobTemplate.Spec, replacements)
		if err != nil {
			return err
		}
		var names []string
		var service map[string]any
		var servers []any
		for _, dep := range jobDeps {
			renamed, err := platform.PreserveSchedulingFields(dep.Raw, []byte(suffixRaw(dep.Raw, replacements)), spec.GangScheduler)
			if err != nil {
				return err
			}
			var obj map[string]any
			if err := json.Unmarshal(renamed, &obj); err != nil {
				return err
			}
			names = append(names, extractMetadataName(renamed))
			resourceSpec, _ := obj["spec"].(map[string]any)
			switch obj["kind"] {
			case "Service":
				service = map[string]any{"resource": extractMetadataName(renamed), "selector": resourceSpec["selector"]}
			case "InferenceWorkload":
				// A port protocol is an enum, never a job-scoped resource reference.
				ports := resourceSpec["ports"].(map[string]any)["items"].(map[string]any)
				require.Contains(tc.T, ports, "http")
				port := ports["http"].(map[string]any)["value"].(map[string]any)
				require.Equal(tc.T, "http", port["protocol"])
				servers = append(servers, resourceSpec)
			case "DistributedInferenceWorkload":
				ports := resourceSpec["servingPorts"].(map[string]any)["items"].(map[string]any)
				require.Contains(tc.T, ports, "http")
				servers = append(servers, obj["leaderSpec"], obj["workerSpec"])
			case "Deployment":
				servers = append(servers, resourceSpec["template"])
			case "LeaderWorkerSet":
				group := resourceSpec["leaderWorkerTemplate"].(map[string]any)
				servers = append(servers, group["leaderTemplate"], group["workerTemplate"])
			}
		}
		// Only retain placement/identity fields; the catalog tests cover server commands.
		var serverMetadata []any
		for _, template := range servers {
			pod := template.(map[string]any)
			if pod["image"] != nil {
				serverMetadata = append(serverMetadata, map[string]any{"labels": pod["labels"], "nodeAffinityRequired": pod["nodeAffinityRequired"], "tolerations": pod["tolerations"]})
				continue
			}
			ps := pod["spec"].(map[string]any)
			serverMetadata = append(serverMetadata, map[string]any{"metadata": pod["metadata"], "scheduler": ps["schedulerName"], "affinity": ps["affinity"], "tolerations": ps["tolerations"]})
		}
		result := map[string]any{"workflowScoped": workflowNames, "jobScoped": names, "service": service, "servers": serverMetadata, "probeEnv": patched.Workload.Inference.Template.Spec.Containers[0].Env, "backend": patched.Backend}
		data, err := json.MarshalIndent(result, "", "  ")
		tc.Actual = string(data)
		return err
	})
}
