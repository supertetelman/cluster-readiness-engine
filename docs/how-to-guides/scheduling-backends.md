# Scheduling backends

Set `backend` on a Job or a Workflow's `spec.jobTemplate.spec`, or on a
WorkloadRun or Certification's spec. Certification category options can
override the global setting. Omitting it preserves existing scheduling.

```yaml
backend:
  name: runai
  queue: my-existing-runai-queue
```

`kubernetes` submits the shared MPIJob directly and accepts no queue. `runai`
submits a built-in `run.ai/v2alpha1` `DistributedWorkload` with `jobType: MPIJob`.
Run:ai creates and owns the MPIJob and schedules its pods. The project namespace
and queue must already exist with sufficient quota, and the MPI Operator must
serve `kubeflow.org/v2beta1/MPIJob`. NVCRE uses its Kubernetes credentials to
create the native Run:ai CRD; no tenant password is stored in NVCRE.

MPIJob workloads use `spec.workload.mpiJob` with the MPI Operator's spec.
NVCRE owns creation and deletion, reports pending time separately from running
time, and uses the same pod logs and bandwidth profiles as TrainJob workloads.
Use `mpiJob` for native Run:ai training. A `trainJob` remains supported through
Kubernetes and, on the Slinky branches, Slurm Bridge. Unsupported combinations
fail before submission instead of falling back to ExternalWorkload integration.

The [shared MPI NCCL example](https://github.com/NVIDIA/cluster-readiness-engine/blob/main/examples/mpi-nccl.yaml)
runs a two-node, one-GPU-per-node NCCL all-reduce test over TCP. Submit it
unchanged for native Kubernetes. For Run:ai change only
`spec.jobTemplate.spec.backend` to the mapping above and use a Run:ai project
namespace. Keep the image, launcher, worker resources, and arguments identical
between runs. Preserve each Workflow, Job, MPIJob, launcher log, and NVCRE
report separately for external CI to consume; NVCRE does not compare runs.

After collecting results, delete the Workflow and wait for its MPIJob and
workers to be removed before starting the next run. A blocked or failed
cluster run is not a successful backend validation.

## Verify the submission

For Kubernetes, expect `NVCRE Job → MPIJob → worker pods / launcher Job`.
For Run:ai, expect `NVCRE Job → DistributedWorkload → MPIJob → pods`.
Check actual owner references and UIDs. An ExternalWorkload or a pod with
`schedulerName: runai-scheduler` does not prove native Run:ai submission.

The shared example uses the same image, commands, environment, GPU requests,
and MPI arguments for both paths. Native Run:ai supports a single container
per role and required node affinity. Unsupported pod features produce a clear
error. Complex literal arguments use Kubernetes environment expansion to preserve
nested quotes and backslashes through Run:ai's string parser. Names beginning
with `NVCRE_COMMAND_` or `NVCRE_ARGUMENTS_` are reserved for this purpose.
In complex arguments, `$(VAR)` references to container environment variables
are rejected because Run:ai does not order that environment map; shell scripts
can use `$VAR` references. Shell command substitution such as `$(date)` is preserved.
The launcher waits for SSH readiness; set the NVCRE execution timeout
rather than MPIJob-only deadlines. On the isolated two-node lab, each worker
requests the single available GPU on its node.

For the Kubernetes baseline, use a namespace outside Run:ai project management:
Run:ai admission may rewrite a pod scheduler in its project namespace. Capture
GPU compute processes while the test runs, launcher logs, NVCRE results, and
cleanup for both backends. Result comparison belongs to external CI.

## Native Run:ai inference

The [vLLM example](https://github.com/NVIDIA/cluster-readiness-engine/blob/main/examples/runai-vllm.yaml)
runs the Workload Kit's single-GPU and two-node distributed scenarios. The
same image, Qwen model, Ray setup, and request probe remain in the shared catalog.

With `backend.name: runai`, NVCRE submits `InferenceWorkload` for the single
server and `DistributedInferenceWorkload` for the leader/worker server. Run:ai
owns the resulting Knative Service or LeaderWorkerSet. Install Knative Serving
for single-server inference and the LWS controller for distributed inference.
The validation lab uses Run:ai 2.26.50, Knative Serving 1.22.0 and LWS 0.9.0.

The finite CPU request probe runs as a native Run:ai `TrainingWorkload`, so its
success means a real chat completion returned nonempty assistant content.
Its image must provide GNU `timeout` (the catalog uses Python slim/coreutils);
the adapter enforces the shared probe deadline inside the container. Server
readiness alone never marks the NVCRE test successful. Native resources,
services and probe ConfigMaps share the Job's dependency lifecycle and cleanup.
Slinky inference is explicitly unsupported; Slinky remains training-only.

## Adding a scheduler or a test

The workload spec describes **what runs**. `ForBackend` selects the resource
submitted to the platform. For another backend:

1. Add its backend name to `SchedulingBackend` in `api/v1alpha1/job_types.go`
   and resolve its metadata in `pkg/workload/backend.go`. If it owns native
   workload CRDs, add its submission adapter in `pkg/workload/adapter.go`.
2. Keep capability restrictions in `pkg/workload/backend.go`. Keep backend-specific allocation metadata in
   `pkg/platform/backend.go`; reuse the existing runtime and pod transforms.
3. Run `make manifests generate` to update the API artifacts. Add scheduling
   fixtures and extend `TestMPIJobBackend` so a new
   backend cannot silently change the shared containers.
4. Validate on the actual scheduler, including its enterprise workload record
   or Slurm allocation, workload completion and cleanup.

A new test usually needs a catalog entry or WorkloadRun with its own command
and log profile, not another backend implementation. Add a workload adapter
only for a new resource type with a different build/status lifecycle.
