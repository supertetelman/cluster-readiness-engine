// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"encoding/json"
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
	if err := workload.ValidateBackendSupport(&spec.JobTemplate.Spec.Workload, backend); err != nil {
		return err
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
		if backend.Name == "slinky" {
			if err := applySlinkyAllocation(spec.Dependencies, gs.Queue); err != nil {
				return err
			}
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

// The bridge reads partition annotations on the generated JobSet, not on its pods.
func applySlinkyAllocation(deps []nvcrev1alpha1.DependencySpec, partition string) error {
	for i := range deps {
		var obj map[string]any
		if err := json.Unmarshal(deps[i].Raw, &obj); err != nil {
			return err
		}
		if obj[keyKind] != kindTrainingRuntime {
			continue
		}
		template := ensureMap(ensureMap(obj, keySpec), keyTemplate)
		annotations := ensureMap(ensureMap(template, keyMetadata), "annotations")
		annotations["slurmjob.slinky.slurm.net/partition"] = partition
		// The CPU-only MPI launcher must fit beside a GPU worker on the same node.
		annotations["slurmjob.slinky.slurm.net/exclusive"] = "false"
		raw, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		deps[i].Raw = raw
	}
	return nil
}
