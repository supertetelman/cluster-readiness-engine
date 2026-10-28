// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

func TestMPIExampleSchema(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "mpi-example"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Example string `json:"example"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		raw, err := os.ReadFile(filepath.Join("..", "..", "examples", input.Example))
		if err != nil {
			return err
		}
		var workflow nvcrev1alpha1.Workflow
		if err := yaml.UnmarshalStrict(raw, &workflow); err != nil {
			return err
		}
		adapter, err := ForSpec(&workflow.Spec.JobTemplate.Spec.Workload)
		if err != nil {
			return err
		}
		nodes, err := adapter.NodesRequired(&workflow.Spec.JobTemplate.Spec.Workload)
		if err != nil {
			return err
		}
		data, err := json.MarshalIndent(map[string]any{"orchestration": workflow.Spec.Orchestration, "nodes": nodes, "backend": workflow.Spec.JobTemplate.Spec.Backend}, "", "  ")
		tc.Actual = string(data)
		return err
	})
}
