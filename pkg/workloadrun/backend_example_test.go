// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workloadrun

import (
	"encoding/json"
	"os"
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// This invariant protects the shared example without copying its container
// definitions into expected fixtures. Scheduling must not change the payload.
func TestTrainJobExampleBackendCapabilities(t *testing.T) {
	raw, err := os.ReadFile("../../examples/mpi-workloadrun.yaml")
	require.NoError(t, err)
	var run nvcrev1alpha1.WorkloadRun
	require.NoError(t, yaml.UnmarshalStrict(raw, &run))
	baseline, err := BuildWorkflowSpec(&run, 1, 0, false, "mpi")
	require.NoError(t, err)
	var want trainerv1alpha1.TrainingRuntime
	require.NoError(t, json.Unmarshal(baseline.Dependencies[0].Raw, &want))

	for _, backend := range []nvcrev1alpha1.SchedulingBackend{
		{Name: "kubernetes"},
		{Name: "runai", Queue: "test-project"},
		{Name: "slinky", Queue: "test-partition"},
	} {
		t.Run(backend.Name, func(t *testing.T) {
			input := run.DeepCopy()
			input.Spec.Backend = &backend
			before := input.DeepCopy()
			got, buildErr := BuildWorkflowSpec(input, 1, 0, false, "mpi")
			if backend.Name == "runai" {
				require.ErrorContains(t, buildErr, "native Run:ai training requires mpiJob")
				return
			}
			require.NoError(t, buildErr)
			require.Equal(t, before, input, "rendering must not mutate the source")
			require.Equal(t, baseline.JobTemplate.Spec.Workload, got.JobTemplate.Spec.Workload)
			require.Equal(t, &backend, got.JobTemplate.Spec.Backend)

			var runtime trainerv1alpha1.TrainingRuntime
			require.NoError(t, json.Unmarshal(got.Dependencies[0].Raw, &runtime))
			require.Len(t, runtime.Spec.Template.Spec.ReplicatedJobs, len(want.Spec.Template.Spec.ReplicatedJobs))
			for i, job := range runtime.Spec.Template.Spec.ReplicatedJobs {
				pod := job.Template.Spec.Template.Spec
				original := want.Spec.Template.Spec.ReplicatedJobs[i].Template.Spec.Template.Spec
				require.Equal(t, original.Containers, pod.Containers, "backend changed %s image, command, environment or resources", job.Name)
				require.Equal(t, original.Volumes, pod.Volumes, "backend changed %s volumes", job.Name)
			}
		})
	}
}
