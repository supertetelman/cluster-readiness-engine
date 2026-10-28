// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// Unsupported fields must fail rather than turn a different payload into a pass.
func TestRunaiPodRejectsUnsupportedFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*corev1.PodTemplateSpec)
		want   string
	}{
		{"sidecar", func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar"})
		}, "one container"},
		{"init-container", func(p *corev1.PodTemplateSpec) { p.Spec.InitContainers = []corev1.Container{{Name: "init"}} }, "initContainers"},
		{"service-account", func(p *corev1.PodTemplateSpec) { p.Spec.ServiceAccountName = "custom" }, "serviceAccountName"},
		{"env-reference", func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "REFERENCE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}
		}, "valueFrom"},
		{"pod-affinity", func(p *corev1.PodTemplateSpec) {
			p.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}}
		}, "arbitrary pod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test"}}}}
			tc.change(pod)
			_, err := RunaiPod(pod)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
