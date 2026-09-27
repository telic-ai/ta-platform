# Execution Sandbox — Kubernetes reference

Reference manifests for running sandbox executions as pods, mirroring what
`internal/sandbox`'s container runner enforces locally. They are not yet
consumed by code: the MVP runner drives a container engine directly
(`NewGVisorRunner`). A pod-per-execution runner implementing the same
`sandbox.Runner` interface is the follow-up.

| Control | Local runner flag | Pod equivalent |
|---|---|---|
| gVisor | `--runtime runsc` | `runtimeClassName: gvisor` |
| Deny-all egress, IMDS | `--network none` | `NetworkPolicy` with no egress rules |
| Wall-clock kill | `docker kill` on timeout | `activeDeadlineSeconds` |
| Non-root | `--user 65534:65534` | `runAsUser/runAsGroup: 65534`, `runAsNonRoot` |
| Read-only root FS | `--read-only` | `readOnlyRootFilesystem: true` |
| No privileges | `--cap-drop ALL`, `no-new-privileges` | `capabilities.drop: [ALL]`, `allowPrivilegeEscalation: false` |
| Seccomp | engine default profile | `seccompProfile: RuntimeDefault` |
| Limits | `--cpus`, `--memory`, `--pids-limit` | `resources.limits`, kubelet `podPidsLimit` |
| No service account | n/a | `automountServiceAccountToken: false` |

Under gVisor, guest processes are not host PIDs, so the local gVisor runner
also sets `RLIMIT_NPROC`. For pods, set it the same way in the image's
entrypoint (`ulimit -u`), because Kubernetes has no rlimit field.
