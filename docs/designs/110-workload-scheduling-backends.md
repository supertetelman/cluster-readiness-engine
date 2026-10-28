# ADR-110: Workload Scheduling Backends

> **Status:** Accepted — approved 2026-10-27; native Run:ai requirement corrected by the user 2026-10-28

## Context

NVCRE currently creates Kubeflow Trainer v2 TrainJobs through its workload
adapter. The requested additions are Run:ai MPI training, Slinky training,
and Run:ai vLLM inference. The same workload commands and reports must remain
usable across training backends. Result comparison belongs to external CI.

## Decision

Keep workload execution in Kubernetes and reuse the existing adapter,
controller lifecycle, pod log collection, and reports. Introduce only the
workload types and scheduling configuration needed by these four branches.

- Preserve the default Kubernetes backend and existing TrainJob behavior.
- Add a typed MPIJob workload adapter. Share its launcher/worker definition
  between native Kubernetes and Run:ai. Kubernetes submits MPIJob directly;
  Run:ai submits its built-in DistributedWorkload (jobType MPIJob), which
  creates and owns the MPIJob. Never substitute ExternalWorkload registration
  or schedulerName routing for native Run:ai workload submission.
- Use Slinky Slurm Bridge for Kubernetes training workloads. Reuse the
  existing TrainJob/JobSet path where supported, with Slurm scheduling
  configuration. Do not invent a SlurmJob CRD or introduce a separate sbatch
  execution service. Confirm the installed bridge and gang scheduling API
  before selecting its exact metadata fields.
- Submit built-in Run:ai InferenceWorkload and DistributedInferenceWorkload
  server resources from the shared vLLM definitions, and bounded verification
  jobs that exercise
  the Workload Kit's single-GPU and distributed vLLM scenarios. A serving
  workload becoming Ready is not the test result: send a real OpenAI-compatible
  chat completion request and validate the response before reporting success.
- Reject inference with the Slinky backend before creating resources. This
  is the support boundary of this NVCRE implementation, not a statement about
  every upstream Slinky capability.

Place backend selection alongside the workload union, rather than adding
properties inside its exactly-one-of validation. Propagate the selection
through the existing Job, Workflow, WorkloadRun, certification, and render
paths that construct workloads. Reuse existing gang scheduler options and
workload metadata where possible rather than adding duplicate settings.

## Implementation

Run:ai adapters translate the shared single-container workload payload into
the installed native CRD contract. Use Kubernetes authentication and native
CRD lifecycle rather than adding tenant credentials to NVCRE. Preserve image,
command arguments, resources, environment, volumes, and placement; fail with
a descriptive error for unsupported fields instead of silently losing them.
Read optional native objects live without starting mandatory informers.
Native training status, pod labels, log discovery, deletion, rendering and
namespace-scoped RBAC must follow the same adapter. Native inference servers
remain job-scoped dependencies, with the existing finite request probe deciding
success. Test native resource kinds and real owner UIDs, as well as GPU compute
processes and output; an ExternalWorkload is not valid native evidence.


| Branch | Base | Scope |
| --- | --- | --- |
| `runai-training` | upstream `main` at `cace7125` | MPIJob adapter, shared native/Run:ai MPI test, scheduling configuration |
| `slinky-training` | `runai-training` | Slinky training configuration and tests |
| `runai-inference` | `runai-training` | Workload Kit vLLM scenarios through Run:ai |
| `slinky-inference` | `runai-inference` plus Slinky training commits | Combined functionality; explicit Slinky inference rejection |

Update API validation and generated schemas/deep copies using the repository
generators; register required object types, watches, and RBAC. Cover workload
creation, status transitions (including pending), deletion, labels, node
placement, resource counts, unsupported combinations, and rendering using the
repository's golden fixture conventions. Keep changes to existing golden
files limited to expected consequences, diagnose differences first, and
review every generated diff.

Run the required build, lint, unit, and envtest checks on each branch. On COD,
run the same MPI test sequentially with native Kubernetes and Run:ai, retaining
the workload logs and native NVCRE reports separately. Run training through a
real Slinky scheduler and vLLM inference through Run:ai. Capture the tested
commit, image, backend, resource outcomes, logs, reports, and cleanup outcome.
Push each branch only after its required cluster validation succeeds.

Use signed, DCO-signed commits as the configured human author. Send branch
creation and branch-testing-complete updates to the existing Everett/Ryan DM,
distinguishing successful validation from blocked or failed attempts.

## Rationale

The existing workload boundary already handles construction and normalized
status. Kubernetes execution preserves pod logs, ownership, cleanup, and
measurement paths across scheduling systems. Slurm Bridge avoids duplicating
those paths in a separate Slurm client.

## Consequences

New CRD fields and workload types require generated schema updates and
controller permissions. Cluster validation requires reachable COD access,
Run:ai projects and quota, the MPI operator, and a usable Slinky Slurm Bridge
cluster. Missing prerequisites block publication of the affected branches;
mock tests alone do not satisfy live validation.

## Alternatives Considered

- Separate catalogs and test implementations per scheduler: duplicates the
  workloads and makes external result comparison harder.
- A general backend plugin framework: adds machinery beyond the requested
  three scheduling paths.
- Native sbatch submission through pod exec: duplicates lifecycle and log
  handling and requires a different execution model.

## Notes

Live validation requires a fresh isolated COD cluster with exactly two
A100 GPU workers and a fresh Run:ai installation. Do not reuse existing labs
or share this task's cluster resources with unrelated work.

The embedded MPIJob spec stays typed in Go and is preserved in the NVCRE CRD;
the MPI Operator owns its schema validation. Duplicating its complete Pod
schemas in NVCRE's Job and Workflow CRDs exceeds Helm's release Secret budget.

## References

- Existing `pkg/workload/adapter.go`, `pkg/platform/gang_scheduler.go`, and
  `pkg/controller/job_controller.go`.
- ADR-079: workload object labels.
- [Slinky Slurm Bridge quickstart](https://github.com/SlinkyProject/slurm-bridge/blob/main/docs/quickstart.md).
- [Kubeflow Trainer Slurm Bridge](https://trainer.kubeflow.org/en/latest/operator-guides/job-scheduling/slurm-bridge.html).
- Run:ai Workload Kit source at
  `src/test/suites/playwright/parallel/lemur/inference/real-gpu/inferenceRealGpu.test.ts`
  in the locally extracted Workload Kit source.
