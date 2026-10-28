// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// RunaiProbeAdapter runs the finite CPU request probe as native Run:ai training.
// The GPU servers are separate InferenceWorkload dependencies of the same Job.
type RunaiProbeAdapter struct{ InferenceAdapter }

func (*RunaiProbeAdapter) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "run.ai", Version: "v2alpha1", Kind: "TrainingWorkload"}
}

func (a *RunaiProbeAdapter) NewObject() client.Object {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(a.GVK())
	return u
}

func (a *RunaiProbeAdapter) Build(name, namespace string, spec *nvcrev1alpha1.WorkloadSpec) (client.Object, error) {
	if _, err := a.InferenceAdapter.Build(name, namespace, spec); err != nil {
		return nil, err
	}
	probe := spec.Inference.DeepCopy()
	c := &probe.Template.Spec.Containers[0]
	if len(c.Command) == 0 {
		return nil, fmt.Errorf("native Run:ai probe requires an explicit command and GNU timeout in its image")
	}
	// TrainingWorkload has no activeDeadlineSeconds. Enforce the shared bound
	// inside the probe container; the catalog's Python slim image has coreutils.
	c.Command = append([]string{"timeout", "--kill-after=10s", strconv.FormatInt(*probe.ActiveDeadlineSeconds, 10)}, c.Command...)
	s, err := RunaiPod(&probe.Template)
	if err != nil {
		return nil, err
	}
	if _, requested := s["gpuDevices"]; !requested {
		s["gpuDevices"] = runaiValue(0)
	}
	if probe.BackoffLimit != nil {
		s["backoffLimit"] = runaiValue(*probe.BackoffLimit)
	}
	return RunaiObject(a.GVK(), name, namespace, s, nil)
}

func (*RunaiProbeAdapter) GetStatus(obj client.Object) (*WorkloadStatus, error) {
	return runaiStatus(obj)
}
