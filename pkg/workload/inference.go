// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"
	"maps"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// InferenceAdapter reports the result of a finite inference request probe.
// Servers are job-scoped dependencies, submitted natively by the chosen backend.
type InferenceAdapter struct{}

var _ Adapter = &InferenceAdapter{}

func (*InferenceAdapter) GVK() schema.GroupVersionKind {
	return batchv1.SchemeGroupVersion.WithKind("Job")
}
func (*InferenceAdapter) NewObject() client.Object { return &batchv1.Job{} }

func (a *InferenceAdapter) Build(name, namespace string, spec *nvcrev1alpha1.WorkloadSpec) (client.Object, error) {
	if spec.Inference == nil || len(spec.Inference.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("inference requires a probe pod template")
	}
	if spec.Inference.ActiveDeadlineSeconds == nil || *spec.Inference.ActiveDeadlineSeconds <= 0 {
		return nil, fmt.Errorf("inference requires a positive activeDeadlineSeconds to bound server readiness and requests")
	}
	return &batchv1.Job{
		APIVersion: a.GVK().GroupVersion().String(), Kind: "Job",
		Name: name, Namespace: namespace, Spec: *spec.Inference.DeepCopy(),
	}, nil
}

func (*InferenceAdapter) InjectPodLabel(spec *nvcrev1alpha1.WorkloadSpec, key, value string) {
	if spec.Inference == nil {
		return
	}
	if spec.Inference.Template.Labels == nil {
		spec.Inference.Template.Labels = map[string]string{}
	}
	spec.Inference.Template.Labels[key] = value
}
func (*InferenceAdapter) SetNodeSelector(spec *nvcrev1alpha1.WorkloadSpec, selector map[string]string) {
	if spec.Inference == nil {
		return
	}
	if spec.Inference.Template.Spec.NodeSelector == nil {
		spec.Inference.Template.Spec.NodeSelector = map[string]string{}
	}
	maps.Copy(spec.Inference.Template.Spec.NodeSelector, selector)
}
func (*InferenceAdapter) SetNodeAffinity(spec *nvcrev1alpha1.WorkloadSpec, affinity *corev1.NodeAffinity) {
	if spec.Inference == nil {
		return
	}
	if spec.Inference.Template.Spec.Affinity == nil {
		spec.Inference.Template.Spec.Affinity = &corev1.Affinity{}
	}
	spec.Inference.Template.Spec.Affinity.NodeAffinity = affinity.DeepCopy()
}
func (*InferenceAdapter) SetTolerations(spec *nvcrev1alpha1.WorkloadSpec, tolerations []corev1.Toleration) {
	if spec.Inference != nil {
		spec.Inference.Template.Spec.Tolerations = append(spec.Inference.Template.Spec.Tolerations, tolerations...)
	}
}
func (*InferenceAdapter) NodesRequired(spec *nvcrev1alpha1.WorkloadSpec) (int, error) {
	if spec.Inference == nil {
		return 0, fmt.Errorf("WorkloadSpec.Inference is nil")
	}
	return 1, nil
}
func (*InferenceAdapter) SetNumNodes(_ *nvcrev1alpha1.WorkloadSpec, _ int) {}

func (*InferenceAdapter) GetStatus(obj client.Object) (*WorkloadStatus, error) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return nil, fmt.Errorf("expected *batch.Job, got %T", obj)
	}
	for _, state := range []struct {
		condition batchv1.JobConditionType
		phase     WorkloadPhase
	}{
		{batchv1.JobFailed, WorkloadFailed},
		{batchv1.JobComplete, WorkloadSucceeded},
		{batchv1.JobSuspended, WorkloadPending},
	} {
		for _, condition := range job.Status.Conditions {
			if condition.Type == state.condition && condition.Status == corev1.ConditionTrue {
				return &WorkloadStatus{Phase: state.phase, Reason: condition.Reason, Message: condition.Message}, nil
			}
		}
	}
	if job.Status.Active > 0 {
		return &WorkloadStatus{Phase: WorkloadRunning}, nil
	}
	return &WorkloadStatus{Phase: WorkloadPending}, nil
}
