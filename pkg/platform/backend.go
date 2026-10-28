// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"fmt"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/workload"
)

// ApplySchedulingBackend keeps backend intent on the Job and uses the existing
// runtime transform for TrainJobs. MPIJobs carry their pod templates directly.
func ApplySchedulingBackend(spec *nvcrev1alpha1.WorkflowSpec, backend *nvcrev1alpha1.SchedulingBackend) error {
	if backend == nil {
		return nil
	}
	gs, err := workload.BackendScheduler(backend)
	if err != nil {
		return err
	}
	if existing := ResolvedGangScheduler(spec.GangScheduler); existing != nil {
		if gs == nil || *existing != *gs {
			return fmt.Errorf("backend conflicts with gangScheduler")
		}
	}
	if _, err := workload.ForBackend(&spec.JobTemplate.Spec.Workload, backend); err != nil {
		return err
	}
	spec.JobTemplate.Spec.Backend = backend.DeepCopy()
	if gs == nil {
		return nil
	}
	if spec.JobTemplate.Spec.Workload.TrainJob != nil {
		if err := ApplyGangSchedulerToDependencies(spec.Dependencies, gs); err != nil {
			return err
		}
		spec.GangScheduler = gs
	}
	labels, err := workload.InsertConsistentLabel(workload.LabelsOf(spec.JobTemplate.Spec.WorkloadMetadata), gs.QueueLabelKey, gs.Queue, JobTemplateWorkloadLabelsPath)
	if err != nil {
		return err
	}
	spec.JobTemplate.Spec.WorkloadMetadata = workload.MetadataFrom(labels)
	return nil
}
