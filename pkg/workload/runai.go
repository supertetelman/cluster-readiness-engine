// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	mpiv2beta1 "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// RunaiMPIAdapter submits the native Run:ai workload, whose controller owns MPIJob.
// Embedding keeps replica counts and source-pod placement shared with Kubernetes.
type RunaiMPIAdapter struct{ MPIJobAdapter }

const runaiItems = "items"

func (*RunaiMPIAdapter) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "run.ai", Version: "v2alpha1", Kind: "DistributedWorkload"}
}

func (a *RunaiMPIAdapter) NewObject() client.Object {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(a.GVK())
	return u
}

func (a *RunaiMPIAdapter) Build(name, namespace string, spec *nvcrev1alpha1.WorkloadSpec) (client.Object, error) {
	if _, err := a.MPIJobAdapter.Build(name, namespace, spec); err != nil {
		return nil, err
	}
	mpi := spec.MPIJob
	if mpi.RunLauncherAsWorker != nil && *mpi.RunLauncherAsWorker {
		return nil, fmt.Errorf("native Run:ai MPI requires a separate launcher")
	}
	if mpi.MPIImplementation != "" && string(mpi.MPIImplementation) != "OpenMPI" {
		return nil, fmt.Errorf("native Run:ai MPI supports OpenMPI")
	}
	worker := mpi.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeWorker]
	launcher := mpi.MPIReplicaSpecs[mpiv2beta1.MPIReplicaTypeLauncher]
	if launcher.Replicas != nil && *launcher.Replicas != 1 {
		return nil, fmt.Errorf("native Run:ai MPI requires one launcher")
	}
	workerSpec, err := RunaiPod(&worker.Template)
	if err != nil {
		return nil, fmt.Errorf("runai worker: %w", err)
	}
	masterSpec, err := RunaiPod(&launcher.Template)
	if err != nil {
		return nil, fmt.Errorf("runai launcher: %w", err)
	}
	workers, _ := a.NodesRequired(spec)
	workerSpec["jobType"] = "MPIJob"
	workerSpec["workers"] = runaiValue(workers)
	workerSpec["restartPolicy"] = runaiValue(string(worker.RestartPolicy))
	masterSpec["restartPolicy"] = runaiValue(string(launcher.RestartPolicy))
	creationPolicy := string(mpi.LauncherCreationPolicy)
	if creationPolicy == "" {
		creationPolicy = "AtStartup"
	}
	mpiFields := map[string]any{"launcherCreationPolicy": runaiValue(creationPolicy)}
	if mpi.SlotsPerWorker != nil {
		mpiFields["slotsPerWorker"] = runaiValue(*mpi.SlotsPerWorker)
	}
	if mpi.SSHAuthMountPath != "" {
		mpiFields["sshAuthMountPath"] = runaiValue(mpi.SSHAuthMountPath)
	}
	workerSpec["mpiJob"] = map[string]any{"values": mpiFields}
	policy := map[string]any{}
	if mpi.RunPolicy.CleanPodPolicy != nil {
		policy["cleanPodPolicy"] = runaiValue(string(*mpi.RunPolicy.CleanPodPolicy))
	}
	if mpi.RunPolicy.BackoffLimit != nil {
		policy["backoffLimit"] = runaiValue(*mpi.RunPolicy.BackoffLimit)
	}
	if mpi.RunPolicy.ActiveDeadlineSeconds != nil || mpi.RunPolicy.TTLSecondsAfterFinished != nil || mpi.RunPolicy.SchedulingPolicy != nil {
		return nil, fmt.Errorf("native Run:ai MPI supports cleanPodPolicy and backoffLimit; set the NVCRE execution timeout for deadlines")
	}
	if mpi.RunPolicy.Suspend != nil {
		workerSpec["active"] = runaiValue(!*mpi.RunPolicy.Suspend)
	}
	if mpi.RunPolicy.ManagedBy != nil {
		return nil, fmt.Errorf("native Run:ai MPI does not delegate managedBy")
	}
	workerSpec["runPolicy"] = map[string]any{"values": policy}
	return RunaiObject(a.GVK(), name, namespace, workerSpec, map[string]any{"masterSpec": masterSpec})
}

func (*RunaiMPIAdapter) GetStatus(obj client.Object) (*WorkloadStatus, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expected native Run:ai workload, got %T", obj)
	}
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	message, _, _ := unstructured.NestedString(u.Object, "status", "phaseMessage")
	status := &WorkloadStatus{Phase: WorkloadPending, Reason: phase, Message: message}
	switch phase {
	case "Failed":
		status.Phase = WorkloadFailed
	case "Completed":
		status.Phase = WorkloadSucceeded
	case "Running":
		status.Phase = WorkloadRunning
	}
	return status, nil
}

func runaiValue(value any) map[string]any { return map[string]any{"value": value} }

// RunaiObject uses only the native CRD contract, without requiring a Run:ai SDK.
func RunaiObject(gvk schema.GroupVersionKind, name, namespace string, spec, fields map[string]any) (*unstructured.Unstructured, error) {
	obj := map[string]any{"apiVersion": gvk.GroupVersion().String(), "kind": gvk.Kind,
		"metadata": map[string]any{"name": name, "namespace": namespace}, "spec": spec}
	maps.Copy(obj, fields)
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	err = json.Unmarshal(data, &u.Object)
	return u, err
}

// RunaiPod translates the shared payload into Run:ai's native fields. Unsupported
// pod features fail explicitly; they must never disappear during translation.
func RunaiPod(pod *corev1.PodTemplateSpec) (map[string]any, error) {
	if len(pod.Spec.Containers) != 1 {
		return nil, fmt.Errorf("native Run:ai requires one container per role")
	}
	if err := supportedFields(pod.Spec, "containers", "nodeSelector", "affinity", "tolerations", "restartPolicy", "imagePullSecrets", "hostIPC", "hostNetwork", "volumes"); err != nil {
		return nil, err
	}
	c := pod.Spec.Containers[0]
	if err := supportedFields(c, "name", "image", "command", "args", "env", "resources", "imagePullPolicy", "workingDir", "volumeMounts", "ports", "readinessProbe"); err != nil {
		return nil, err
	}
	env := map[string]any{}
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			return nil, fmt.Errorf("native Run:ai environment valueFrom is unsupported: %s", e.Name)
		}
		env[e.Name] = runaiValue(e.Value)
	}
	s := map[string]any{"image": runaiValue(c.Image)}
	for key, argv := range map[string][]string{"command": c.Command, "arguments": c.Args} {
		if len(argv) != 0 {
			commandLine, err := runaiCommandLine(key, argv, env)
			if err != nil {
				return nil, err
			}
			s[key] = runaiValue(commandLine)
		}
	}
	for key, value := range map[string]string{"workingDir": c.WorkingDir, "imagePullPolicy": string(c.ImagePullPolicy), "restartPolicy": string(pod.Spec.RestartPolicy)} {
		if value != "" {
			s[key] = runaiValue(value)
		}
	}
	for _, resources := range []struct {
		values corev1.ResourceList
		limit  bool
	}{{c.Resources.Requests, false}, {c.Resources.Limits, true}} {
		for name, quantity := range resources.values {
			key := string(name)
			switch name {
			case corev1.ResourceCPU, corev1.ResourceMemory:
				if resources.limit {
					key += "Limit"
				}
				value := quantity.String()
				if name == corev1.ResourceCPU {
					value = strconv.FormatFloat(quantity.AsApproximateFloat64(), 'f', -1, 64)
				}
				s[key] = runaiValue(value)
			case "nvidia.com/gpu":
				count, ok := quantity.AsInt64()
				if !ok || count < 0 {
					return nil, fmt.Errorf("native Run:ai requires an integer GPU count")
				}
				s["gpuDevices"] = runaiValue(count)
			default:
				return nil, fmt.Errorf("native Run:ai resource %q is unsupported", name)
			}
		}
	}
	for key, values := range map[string]map[string]string{"labels": pod.Labels, "annotations": pod.Annotations} {
		items := map[string]any{}
		for k, v := range values {
			items[k] = runaiValue(v)
		}
		if len(items) != 0 {
			s[key] = map[string]any{runaiItems: items}
		}
	}
	if len(env) != 0 {
		s["environment"] = map[string]any{runaiItems: env}
	}
	if err := runaiPlacement(s, &pod.Spec); err != nil {
		return nil, err
	}
	if len(pod.Spec.Volumes) != 0 || len(c.VolumeMounts) != 0 || len(c.Ports) != 0 || c.ReadinessProbe != nil {
		return nil, fmt.Errorf("this Run:ai adapter cannot translate pod volumes, container ports or readiness probes")
	}
	return s, nil
}

func runaiPlacement(s map[string]any, pod *corev1.PodSpec) error {
	affinity := &corev1.NodeSelector{}
	if pod.Affinity != nil {
		if pod.Affinity.PodAffinity != nil || pod.Affinity.PodAntiAffinity != nil || (pod.Affinity.NodeAffinity != nil && len(pod.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0) {
			return fmt.Errorf("native Run:ai supports required node affinity, not arbitrary pod or preferred affinity")
		}
		if pod.Affinity.NodeAffinity != nil && pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			affinity = pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.DeepCopy()
		}
	}
	if len(pod.NodeSelector) != 0 && len(affinity.NodeSelectorTerms) == 0 {
		affinity.NodeSelectorTerms = []corev1.NodeSelectorTerm{{}}
	}
	for i := range affinity.NodeSelectorTerms {
		if len(affinity.NodeSelectorTerms[i].MatchFields) != 0 {
			return fmt.Errorf("native Run:ai supports node label expressions, not matchFields")
		}
		for _, key := range slices.Sorted(maps.Keys(pod.NodeSelector)) {
			value := pod.NodeSelector[key]
			affinity.NodeSelectorTerms[i].MatchExpressions = append(affinity.NodeSelectorTerms[i].MatchExpressions, corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value}})
		}
	}
	if len(affinity.NodeSelectorTerms) != 0 {
		s["nodeAffinityRequired"] = affinity
	}
	items := map[string]any{}
	for i, t := range pod.Tolerations {
		value := map[string]any{"tolerationKey": t.Key, "operator": string(t.Operator)}
		if t.Value != "" {
			value["tolerationValue"] = t.Value
		}
		if t.Effect != "" {
			value["effect"] = string(t.Effect)
		}
		if t.TolerationSeconds != nil {
			value["seconds"] = *t.TolerationSeconds
		}
		items[fmt.Sprint(i)] = runaiValue(value)
	}
	if len(items) != 0 {
		s["tolerations"] = map[string]any{runaiItems: items}
	}
	if len(pod.ImagePullSecrets) > 1 {
		return fmt.Errorf("native Run:ai accepts a single imagePullSecret")
	}
	if len(pod.ImagePullSecrets) == 1 {
		s["imagePullSecrets"] = runaiValue(pod.ImagePullSecrets[0].Name)
	}
	if pod.HostIPC {
		s["hostIpc"] = runaiValue(true)
	}
	if pod.HostNetwork {
		s["hostNetwork"] = runaiValue(true)
	}
	return nil
}

func supportedFields(value any, allowed ...string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var unsupported []string
	for key, v := range fields {
		if !slices.Contains(allowed, key) && v != nil {
			unsupported = append(unsupported, key)
		}
	}
	if len(unsupported) != 0 {
		slices.Sort(unsupported)
		return fmt.Errorf("native Run:ai cannot translate fields: %s", strings.Join(unsupported, ", "))
	}
	return nil
}
