// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/catalog"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/platform"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/workload"
)

func TestVLLMCatalog(t *testing.T) {
	p := &testutil.TestCaseParser{Subdir: "vllm"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Variant string                           `json:"variant"`
			Backend *nvcrev1alpha1.SchedulingBackend `json:"backend"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		entry := catalog.Lookup("inference", input.Variant)
		require.NotNil(tc.T, entry)
		spec, err := entry.Build(nvcrev1alpha1.TargetSpec{}, catalog.BuildConfig{NodesPerJob: 1, GpusPerNode: 1, GPUArchitecture: "a100", ImagePullSecrets: []corev1.LocalObjectReference{{Name: "test-pull"}}})
		if err != nil {
			return err
		}
		if err := platform.ApplySchedulingBackend(&spec, input.Backend); err != nil {
			return err
		}
		adapter, err := workload.ForBackend(&spec.JobTemplate.Spec.Workload, input.Backend)
		if err != nil {
			return err
		}
		probe, err := workload.BuildObject(adapter, "probe", "test", &spec.JobTemplate.Spec.Workload, spec.JobTemplate.Spec.WorkloadMetadata, input.Backend)
		if err != nil {
			return err
		}
		b, err := json.MarshalIndent(map[string]any{"workflow": spec, "probe": probe}, "", "  ")
		tc.Actual = string(b)
		return err
	})
}

// Exercise the actual shared probe against a real HTTP server, including failure responses.
func TestVLLMProbe(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err)
	script, err := catalog.EntriesFS().ReadFile("entries/_lib/inference/vllm-probe.py")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "probe.py")
	require.NoError(t, os.WriteFile(path, script, 0o600))
	p := &testutil.TestCaseParser{Subdir: "vllm-probe"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Response json.RawMessage `json:"response"`
			Status   int             `json:"status"`
		}
		if err := json.Unmarshal([]byte(tc.Inputs["input.json"]), &input); err != nil {
			return err
		}
		var request map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path != "/v1/chat/completions" || r.Method != http.MethodPost {
				http.Error(w, "unexpected request", http.StatusNotFound)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(input.Status)
			_, _ = w.Write(input.Response)
		}))
		defer server.Close()
		cmd := exec.Command(python, path)
		cmd.Env = append(os.Environ(), "VLLM_URL="+server.URL, "TIMEOUT_SECONDS=5")
		output, runErr := cmd.Output()
		b, err := json.MarshalIndent(map[string]any{"passed": runErr == nil, "output": string(output), "request": request}, "", "  ")
		tc.Actual = string(b)
		return err
	})
}
