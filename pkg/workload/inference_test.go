// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"encoding/json"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

func TestInferenceBackend(t *testing.T) {
	const nativePhaseField = "phase"
	p := &testutil.TestCaseParser{Subdir: "inference-backend"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Spec   nvcrev1alpha1.JobSpec `json:"spec"`
			Status batchv1.JobStatus     `json:"status"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		adapter, err := ForBackend(&input.Spec.Workload, input.Spec.Backend)
		if err != nil {
			return err
		}
		obj, err := BuildObject(adapter, "probe", "test", &input.Spec.Workload, input.Spec.WorkloadMetadata, input.Spec.Backend)
		if err != nil {
			return err
		}
		if batch, ok := obj.(*batchv1.Job); ok {
			batch.Status = input.Status
		} else {
			phase := "Pending"
			if input.Status.Active > 0 {
				phase = "Running"
			}
			for _, c := range input.Status.Conditions {
				if c.Status == corev1.ConditionTrue {
					switch c.Type {
					case batchv1.JobComplete:
						phase = "Completed"
					case batchv1.JobFailed:
						phase = "Failed"
					case batchv1.JobSuspended:
						phase = "Suspended"
					}
				}
			}
			obj.(*unstructured.Unstructured).Object["status"] = map[string]any{nativePhaseField: phase}
		}
		status, err := adapter.GetStatus(obj)
		if err != nil {
			return err
		}
		b, err := json.MarshalIndent(map[string]any{"probe": obj, "status": status}, "", "  ")
		tc.Actual = string(b)
		return err
	})
}
