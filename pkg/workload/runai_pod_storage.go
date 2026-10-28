// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// runaiVolumes maps only the storage used by the shared vLLM servers and probe.
func runaiVolumes(s map[string]any, pod *corev1.PodTemplateSpec) error {
	volumes := map[string]corev1.Volume{}
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		volume, ok := volumes[mount.Name]
		if !ok || mount.SubPathExpr != "" || mount.MountPropagation != nil {
			return fmt.Errorf("native Run:ai cannot translate volume mount %q", mount.Name)
		}
		var field string
		value := map[string]any{}
		switch {
		case volume.EmptyDir != nil:
			if mount.ReadOnly || mount.SubPath != "" {
				return fmt.Errorf("native Run:ai emptyDir does not support readOnly or subPath")
			}
			field = "emptyDirVolumes"
			value["path"] = mount.MountPath
			value["medium"] = string(volume.EmptyDir.Medium)
			if volume.EmptyDir.SizeLimit != nil {
				value["sizeLimit"] = volume.EmptyDir.SizeLimit.String()
			}
		case volume.ConfigMap != nil:
			if len(volume.ConfigMap.Items) != 0 || (volume.ConfigMap.Optional != nil && *volume.ConfigMap.Optional) {
				return fmt.Errorf("native Run:ai configMap item projection or optional mounts are unsupported")
			}
			field = "configMapVolumes"
			value["name"] = volume.ConfigMap.Name
			value["mountPath"] = mount.MountPath
			if mount.SubPath != "" {
				value["subPath"] = mount.SubPath
			}
			if volume.ConfigMap.DefaultMode != nil {
				value["defaultMode"] = fmt.Sprintf("%04o", *volume.ConfigMap.DefaultMode)
			}
		default:
			return fmt.Errorf("native Run:ai volume %q requires an emptyDir or ConfigMap", volume.Name)
		}
		collection, _ := s[field].(map[string]any)
		if collection == nil {
			collection = map[string]any{runaiItems: map[string]any{}}
			s[field] = collection
		}
		collection[runaiItems].(map[string]any)[mount.Name] = runaiValue(value)
	}
	return nil
}

func runaiReadiness(s map[string]any, probe *corev1.Probe) error {
	if probe == nil {
		return nil
	}
	if probe.HTTPGet == nil || probe.HTTPGet.Port.IntVal == 0 || len(probe.HTTPGet.HTTPHeaders) != 0 {
		return fmt.Errorf("native Run:ai requires an HTTP readiness probe with a numeric port and no custom headers")
	}
	values := map[string]any{"httpGetPort": runaiValue(probe.HTTPGet.Port.IntVal), "httpGetPath": runaiValue(probe.HTTPGet.Path)}
	for key, value := range map[string]int32{"initialDelaySeconds": probe.InitialDelaySeconds, "periodSeconds": probe.PeriodSeconds, "timeoutSeconds": probe.TimeoutSeconds, "successThreshold": probe.SuccessThreshold, "failureThreshold": probe.FailureThreshold} {
		if value != 0 {
			values[key] = runaiValue(value)
		}
	}
	if probe.HTTPGet.Host != "" {
		values["httpGetHost"] = runaiValue(probe.HTTPGet.Host)
	}
	if probe.HTTPGet.Scheme != "" {
		values["httpGetScheme"] = runaiValue(string(probe.HTTPGet.Scheme))
	}
	s["readinessProbe"] = map[string]any{runaiValues: values}
	return nil
}
