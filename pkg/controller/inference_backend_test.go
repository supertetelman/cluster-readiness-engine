// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/workload"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestInferenceBackendLifecycle(t *testing.T) {
	const nativePhaseField = "phase"
	p := &testutil.TestCaseParser{Subdir: "inference-backend-lifecycle"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Spec    nvcrev1alpha1.JobSpec    `json:"spec"`
			Outcome batchv1.JobConditionType `json:"outcome"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		scheme := runtime.NewScheme()
		require.NoError(tc.T, nvcrev1alpha1.AddToScheme(scheme))
		require.NoError(tc.T, batchv1.AddToScheme(scheme))
		require.NoError(tc.T, corev1.AddToScheme(scheme))
		job := &nvcrev1alpha1.Job{Name: "inference-test", Namespace: "test", UID: "test-job", Spec: input.Spec}
		var deletionPropagation *metav1.DeletionPropagation
		native := (&workload.RunaiProbeAdapter{}).NewObject()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job).
			WithStatusSubresource(&nvcrev1alpha1.Job{}, &batchv1.Job{}, native).
			WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if obj.GetName() == "inference-test-workload" {
					options := (&client.DeleteOptions{}).ApplyOptions(opts)
					deletionPropagation = options.PropagationPolicy
				}
				return c.Delete(ctx, obj, opts...)
			}}).Build()
		r := &JobReconciler{Client: c, APIReader: c, Scheme: scheme}
		ctx := context.Background()
		_, err := r.createWorkloadFromSpec(ctx, job)
		if err != nil {
			return err
		}
		ref := job.Status.WorkloadRef
		require.NotNil(tc.T, ref)
		adapter, err := workload.ForBackend(&job.Spec.Workload, job.Spec.Backend)
		if err != nil {
			return err
		}
		mpi := adapter.NewObject()
		key := client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}
		if err := c.Get(ctx, key, mpi); err != nil {
			return err
		}
		require.True(tc.T, metav1.IsControlledBy(mpi, job))
		_, err = r.updateStatusFromWorkload(ctx, job)
		if err != nil {
			return err
		}
		queued := meta.FindStatusCondition(job.Status.Conditions, nvcrev1alpha1.JobInProgress)
		require.NotNil(tc.T, queued)
		require.Nil(tc.T, job.Status.WorkloadStartTime, "pending time must not start the execution timeout")
		pendingReason := queued.Reason
		if batch, ok := mpi.(*batchv1.Job); ok {
			batch.Status.Conditions = []batchv1.JobCondition{{Type: input.Outcome, Status: corev1.ConditionTrue}}
		} else {
			phase := "Failed"
			if input.Outcome == batchv1.JobComplete {
				phase = "Completed"
			}
			if err := unstructured.SetNestedField(mpi.(*unstructured.Unstructured).Object, phase, "status", nativePhaseField); err != nil {
				return err
			}
		}
		if err := c.Status().Update(ctx, mpi); err != nil {
			return err
		}
		_, err = r.updateStatusFromWorkload(ctx, job)
		if err != nil {
			return err
		}
		outcome := nvcrev1alpha1.JobFailed
		if input.Outcome == batchv1.JobComplete {
			outcome = nvcrev1alpha1.JobSucceeded
		}
		condition := meta.FindStatusCondition(job.Status.Conditions, outcome)
		require.NotNil(tc.T, condition)
		if err := r.deleteWorkloadByRef(ctx, job); err != nil {
			return err
		}
		deleted := apierrors.IsNotFound(c.Get(ctx, key, adapter.NewObject()))
		b, err := json.MarshalIndent(map[string]any{"kind": ref.Kind, "pendingReason": pendingReason, "outcome": condition.Type, "status": condition.Status, "deleted": deleted, "deletionPropagation": deletionPropagation}, "", "  ")
		tc.Actual = string(b)
		return err
	})
}
