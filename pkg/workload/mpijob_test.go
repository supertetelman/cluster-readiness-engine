// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"encoding/json"
	"testing"

	mpiv2beta1 "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

func TestMPIJobBackend(t *testing.T) {
	p := &testutil.TestCaseParser{Subdir: "mpi-backend"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Job    nvcrev1alpha1.JobSpec `json:"job"`
			Status mpiv2beta1.JobStatus  `json:"status"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		original, err := json.Marshal(input.Job)
		if err != nil {
			return err
		}
		spec := input.Job.Workload.DeepCopy()
		adapter, err := ForBackend(spec, input.Job.Backend)
		if err != nil {
			return err
		}
		adapter.InjectPodLabel(spec, "nvcre.nvidia.com/job", "test")
		InjectLauncherPodLabel(spec, "nvcre.nvidia.com/launcher", "test")
		adapter.SetNodeSelector(spec, map[string]string{"pool": "gpu"})
		adapter.SetTolerations(spec, []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}})
		obj, err := BuildObject(adapter, "test-workload", "test", spec, input.Job.WorkloadMetadata, input.Job.Backend)
		if err != nil {
			return err
		}
		if job, ok := obj.(*mpiv2beta1.MPIJob); ok {
			job.Status = input.Status
		} else {
			native := obj.(*unstructured.Unstructured)
			phase := "Pending"
			for _, c := range input.Status.Conditions {
				if c.Status == corev1.ConditionTrue {
					phase = string(c.Type)
					if phase == "Succeeded" {
						phase = "Completed"
					}
				}
			}
			native.Object["status"] = map[string]any{"phase": phase}
		}
		status, err := adapter.GetStatus(obj)
		if err != nil {
			return err
		}
		nodes, err := adapter.NodesRequired(spec)
		if err != nil {
			return err
		}
		b, err := json.MarshalIndent(map[string]any{"workload": obj, "status": status, "nodes": nodes}, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b)
		after, err := json.Marshal(input.Job)
		if err != nil {
			return err
		}
		require.JSONEq(tc.T, string(original), string(after), "building must not mutate shared workload definitions")
		return nil
	})
}

func TestMPIJobReplicaCount(t *testing.T) {
	n := int32(3)
	spec := &nvcrev1alpha1.WorkloadSpec{MPIJob: &mpiv2beta1.MPIJobSpec{
		MPIReplicaSpecs: map[mpiv2beta1.MPIReplicaType]*mpiv2beta1.ReplicaSpec{
			mpiv2beta1.MPIReplicaTypeWorker: {Replicas: &n, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test"}}}}},
		},
	}}
	a := &MPIJobAdapter{}
	nodes, err := a.NodesRequired(spec)
	require.NoError(t, err)
	require.Equal(t, 3, nodes)
	a.SetNumNodes(spec, 2)
	nodes, err = a.NodesRequired(spec)
	require.NoError(t, err)
	require.Equal(t, 2, nodes)
	_, err = a.GetStatus(&corev1.Pod{})
	require.Error(t, err)
}
