// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"
	"maps"

	mpiv2beta1 "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// MPIJobAdapter shares one MPI definition between Kubernetes schedulers.
type MPIJobAdapter struct{}

var _ Adapter = &MPIJobAdapter{}

func (*MPIJobAdapter) GVK() schema.GroupVersionKind {
	return mpiv2beta1.SchemeGroupVersion.WithKind("MPIJob")
}

func (*MPIJobAdapter) NewObject() client.Object { return &mpiv2beta1.MPIJob{} }

func (a *MPIJobAdapter) Build(name, namespace string, spec *nvcrev1alpha1.WorkloadSpec) (client.Object, error) {
	if _, err := a.NodesRequired(spec); err != nil {
		return nil, err
	}
	launcher := spec.MPIJob.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeLauncher]
	if launcher == nil || len(launcher.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("mpiJob requires a Launcher pod template")
	}
	return &mpiv2beta1.MPIJob{
		APIVersion: a.GVK().GroupVersion().String(), Kind: "MPIJob",
		Name: name, Namespace: namespace,
		Spec: *spec.MPIJob.DeepCopy(),
	}, nil
}

func (*MPIJobAdapter) InjectPodLabel(spec *nvcrev1alpha1.WorkloadSpec, key, value string) {
	if spec.MPIJob == nil {
		return
	}
	if worker := spec.MPIJob.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeWorker]; worker != nil {
		if worker.Template.Labels == nil {
			worker.Template.Labels = map[string]string{}
		}
		worker.Template.Labels[key] = value
	}
}

func (*MPIJobAdapter) SetNodeSelector(spec *nvcrev1alpha1.WorkloadSpec, selector map[string]string) {
	forEachMPIPod(spec, func(pod *corev1.PodTemplateSpec) {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}
		maps.Copy(pod.Spec.NodeSelector, selector)
	})
}

func (*MPIJobAdapter) SetNodeAffinity(spec *nvcrev1alpha1.WorkloadSpec, affinity *corev1.NodeAffinity) {
	forEachMPIPod(spec, func(pod *corev1.PodTemplateSpec) {
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &corev1.Affinity{}
		}
		pod.Spec.Affinity.NodeAffinity = affinity.DeepCopy()
	})
}

func (*MPIJobAdapter) SetTolerations(spec *nvcrev1alpha1.WorkloadSpec, tolerations []corev1.Toleration) {
	forEachMPIPod(spec, func(pod *corev1.PodTemplateSpec) {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, tolerations...)
	})
}

func (*MPIJobAdapter) NodesRequired(spec *nvcrev1alpha1.WorkloadSpec) (int, error) {
	if spec.MPIJob == nil {
		return 0, fmt.Errorf("WorkloadSpec.MPIJob is nil")
	}
	worker := spec.MPIJob.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeWorker]
	if worker == nil || len(worker.Template.Spec.Containers) == 0 {
		return 0, fmt.Errorf("mpiJob requires a Worker pod template")
	}
	n := int32(1)
	if worker.Replicas != nil {
		n = *worker.Replicas
	}
	if n < 1 {
		return 0, fmt.Errorf("mpiJob Worker replicas must be positive")
	}
	if spec.MPIJob.RunLauncherAsWorker != nil && *spec.MPIJob.RunLauncherAsWorker {
		n++
	}
	return int(n), nil
}

func (*MPIJobAdapter) SetNumNodes(spec *nvcrev1alpha1.WorkloadSpec, numNodes int) {
	if spec.MPIJob == nil {
		return
	}
	if spec.MPIJob.RunLauncherAsWorker != nil && *spec.MPIJob.RunLauncherAsWorker {
		numNodes--
	}
	if worker := spec.MPIJob.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeWorker]; worker != nil {
		n := int32(numNodes)
		worker.Replicas = &n
	}
}

func (*MPIJobAdapter) GetStatus(obj client.Object) (*WorkloadStatus, error) {
	job, ok := obj.(*mpiv2beta1.MPIJob)
	if !ok {
		return nil, fmt.Errorf("expected *MPIJob, got %T", obj)
	}
	for _, state := range []struct {
		condition mpiv2beta1.JobConditionType
		phase     WorkloadPhase
	}{
		{mpiv2beta1.JobFailed, WorkloadFailed},
		{mpiv2beta1.JobSucceeded, WorkloadSucceeded},
		{mpiv2beta1.JobSuspended, WorkloadPending},
		{mpiv2beta1.JobRunning, WorkloadRunning},
	} {
		for _, condition := range job.Status.Conditions {
			if condition.Type == state.condition && condition.Status == corev1.ConditionTrue {
				return &WorkloadStatus{Phase: state.phase, Reason: condition.Reason, Message: condition.Message}, nil
			}
		}
	}
	return &WorkloadStatus{Phase: WorkloadPending}, nil
}

func forEachMPIPod(spec *nvcrev1alpha1.WorkloadSpec, f func(*corev1.PodTemplateSpec)) {
	if spec.MPIJob == nil {
		return
	}
	for _, replica := range spec.MPIJob.MPIReplicaSpecs {
		if replica != nil {
			f(&replica.Template)
		}
	}
}
