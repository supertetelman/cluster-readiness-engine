// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// BackendScheduler resolves the small backend API into existing gang scheduler settings.
func BackendScheduler(backend *nvcrev1alpha1.SchedulingBackend) (*nvcrev1alpha1.GangSchedulerSpec, error) {
	if backend == nil {
		return nil, nil
	}
	switch backend.Name {
	case "kubernetes":
		if backend.Queue != "" {
			return nil, fmt.Errorf("kubernetes backend does not accept a queue")
		}
		return nil, nil
	case "runai":
		if backend.Queue == "" || len(validation.IsValidLabelValue(backend.Queue)) != 0 {
			return nil, fmt.Errorf("runai backend requires an existing queue with a valid Kubernetes label value")
		}
		return &nvcrev1alpha1.GangSchedulerSpec{SchedulerName: "runai-scheduler", Queue: backend.Queue, QueueLabelKey: "runai/queue"}, nil
	default:
		return nil, fmt.Errorf("unsupported workload backend %q", backend.Name)
	}
}

// ValidateBackendSupport rejects unsupported platform/workload combinations before creation.
func ValidateBackendSupport(spec *nvcrev1alpha1.WorkloadSpec, backend *nvcrev1alpha1.SchedulingBackend) error {
	if backend != nil && backend.Name == "slinky" && spec.Inference != nil {
		return fmt.Errorf("inference is unsupported on Slinky; Slinky is training-only")
	}
	return nil
}

// ApplyBackend adds project routing to a native Run:ai workload.
func ApplyBackend(obj client.Object, backend *nvcrev1alpha1.SchedulingBackend) error {
	gs, err := BackendScheduler(backend)
	if err != nil {
		return err
	}
	if gs == nil {
		return nil
	}
	native, ok := obj.(*unstructured.Unstructured)
	if !ok || (native.GroupVersionKind() != (&RunaiMPIAdapter{}).GVK() && native.GroupVersionKind() != (&RunaiProbeAdapter{}).GVK()) {
		return fmt.Errorf("runai backend requires a native Run:ai workload, not scheduler routing on %T", obj)
	}
	labels, err := InsertConsistentLabel(obj.GetLabels(), gs.QueueLabelKey, gs.Queue, "spec.backend.queue")
	if err != nil {
		return err
	}
	obj.SetLabels(labels)
	return nil
}
