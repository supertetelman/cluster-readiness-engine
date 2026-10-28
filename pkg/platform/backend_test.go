// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"encoding/json"
	"testing"

	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

func TestSchedulingBackend(t *testing.T) {
	p := &testutil.TestCaseParser{Subdir: "scheduling-backend"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Workflow nvcrev1alpha1.WorkflowSpec       `json:"workflow"`
			Backend  *nvcrev1alpha1.SchedulingBackend `json:"backend"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		if err := ApplySchedulingBackend(&input.Workflow, input.Backend); err != nil {
			return err
		}
		b, err := json.MarshalIndent(input.Workflow, "", "  ")
		tc.Actual = string(b)
		return err
	})
}
