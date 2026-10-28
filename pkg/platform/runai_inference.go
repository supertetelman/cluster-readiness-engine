// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/workload"
)

const (
	kindRunaiInference            = "InferenceWorkload"
	kindRunaiDistributedInference = "DistributedInferenceWorkload"
	runaiHTTP                     = "http"
)

func runaiField(value any) map[string]any { return map[string]any{"value": value} }

// Shared server definitions become built-in Run:ai resources at submission.
// Run:ai then creates and owns Knative Service or LeaderWorkerSet children.
func applyRunaiInference(deps []nvcrev1alpha1.DependencySpec, queue string) error {
	for i := range deps {
		var obj map[string]any
		if err := json.Unmarshal(deps[i].Raw, &obj); err != nil {
			return err
		}
		var kind, version string
		var spec, fields map[string]any
		switch obj[keyKind] {
		case "Deployment":
			var deployment appsv1.Deployment
			if err := json.Unmarshal(deps[i].Raw, &deployment); err != nil {
				return err
			}
			var err error
			spec, err = runaiServerPod(&deployment.Spec.Template)
			if err != nil {
				return err
			}
			replicas := int32(1)
			if deployment.Spec.Replicas != nil {
				replicas = *deployment.Spec.Replicas
			}
			port, err := servingPort(&deployment.Spec.Template)
			if err != nil {
				return err
			}
			spec["minScale"], spec["maxScale"] = runaiField(replicas), runaiField(replicas)
			spec["ports"] = map[string]any{"items": map[string]any{runaiHTTP: runaiField(map[string]any{"container": port, "protocol": runaiHTTP, "serviceType": "ServingPort"})}}
			kind, version = kindRunaiInference, "v2alpha1"
		case "LeaderWorkerSet":
			var definition struct {
				Spec struct {
					Replicas *int32 `json:"replicas"`
					Group    struct {
						Size          int32                  `json:"size"`
						RestartPolicy string                 `json:"restartPolicy"`
						Leader        corev1.PodTemplateSpec `json:"leaderTemplate"`
						Worker        corev1.PodTemplateSpec `json:"workerTemplate"`
					} `json:"leaderWorkerTemplate"`
				} `json:"spec"`
			}
			if err := json.Unmarshal(deps[i].Raw, &definition); err != nil {
				return err
			}
			group := definition.Spec.Group
			if group.Size < 2 {
				return fmt.Errorf("distributed inference requires a leader and at least one worker")
			}
			leader, err := runaiServerPod(&group.Leader)
			if err != nil {
				return err
			}
			worker, err := workload.RunaiPod(&group.Worker)
			if err != nil {
				return err
			}
			port, err := servingPort(&group.Leader)
			if err != nil {
				return err
			}
			replicas := int32(1)
			if definition.Spec.Replicas != nil {
				replicas = *definition.Spec.Replicas
			}
			spec = map[string]any{"replicas": runaiField(replicas), "workers": runaiField(group.Size - 1), "startupPolicy": runaiField("LeaderCreated")}
			if group.RestartPolicy != "" {
				spec["restartPolicy"] = runaiField(group.RestartPolicy)
			}
			spec["servingPorts"] = map[string]any{"items": map[string]any{runaiHTTP: runaiField(map[string]any{"port": port, "exposeExternally": false})}}
			fields = map[string]any{"leaderSpec": leader, "workerSpec": worker}
			kind, version = kindRunaiDistributedInference, "v1alpha1"
		default:
			continue // Includes native resources on a repeated transform.
		}
		metadata := ensureMap(obj, keyMetadata)
		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)
		native, err := workload.RunaiObject(schema.GroupVersionKind{Group: "run.ai", Version: version, Kind: kind}, name, namespace, spec, fields)
		if err != nil {
			return err
		}
		native.Object[keyMetadata] = metadata
		labels, err := workload.InsertConsistentLabel(native.GetLabels(), "runai/queue", queue, "inference server labels")
		if err != nil {
			return err
		}
		native.SetLabels(labels)
		raw, err := json.Marshal(native.Object)
		if err != nil {
			return err
		}
		deps[i].Raw = raw
	}
	return nil
}

func runaiServerPod(pod *corev1.PodTemplateSpec) (map[string]any, error) {
	copy := pod.DeepCopy()
	if len(copy.Spec.Containers) != 1 {
		return nil, fmt.Errorf("native Run:ai inference requires one container per role")
	}
	// The native server's serving-port field creates the port, not the role spec.
	copy.Spec.Containers[0].Ports = nil
	return workload.RunaiPod(copy)
}

func servingPort(pod *corev1.PodTemplateSpec) (int32, error) {
	if len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Ports) != 1 {
		return 0, fmt.Errorf("native Run:ai inference requires one HTTP serving port")
	}
	return pod.Spec.Containers[0].Ports[0].ContainerPort, nil
}

// Native ports contain enums and internal names, never resource references.
// A Service's port name can otherwise make "http" look like a cross-reference.
func preserveRunaiServingFields(before, after map[string]any) ([]byte, error) {
	src, srcOK := nestedMap(before, keySpec)
	dst, dstOK := nestedMap(after, keySpec)
	if srcOK && dstOK {
		for _, field := range []string{"ports", "servingPorts"} {
			if value, exists := src[field]; exists {
				dst[field] = value
			}
		}
	}
	restoreLabel(before, after, "runai/queue")
	return json.Marshal(after)
}
