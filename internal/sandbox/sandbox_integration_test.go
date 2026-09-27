//go:build integration

package sandbox

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// runtimes returns the runners available on this host. gVisor is skipped,
// not failed, when runsc is not registered with the engine.
func runtimes(t *testing.T) map[string]*ContainerRunner {
	t.Helper()
	out, err := exec.Command("docker", "info", "--format", "{{json .Runtimes}}").Output()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	for _, image := range []string{"python:3.12-alpine", "alpine:3.20"} {
		if exec.Command("docker", "image", "inspect", image).Run() != nil {
			t.Skipf("image %s not pulled", image)
		}
	}
	runners := map[string]*ContainerRunner{RuntimeRunc: NewDockerRunner(ContainerConfig{})}
	if bytes.Contains(out, []byte(`"runsc"`)) {
		runners[RuntimeGVisor] = NewGVisorRunner(ContainerConfig{})
	} else {
		t.Log("runsc not registered; gVisor cases skipped")
	}
	return runners
}

func python(source string) Spec {
	return Spec{ExecutionID: "it-" + uuid.NewString()[:12], Language: "python", Entrypoint: "main.py",
		Files: map[string][]byte{"main.py": []byte(source)}}
}

func mustRun(t *testing.T, runner Runner, spec Spec) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	result, err := runner.Run(ctx, spec)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return result
}

func containerExists(t *testing.T, spec Spec) bool {
	t.Helper()
	out, _ := exec.Command("docker", "ps", "--all", "--quiet", "--filter", "name=^"+containerPrefix+spec.ExecutionID+"$").Output()
	return len(bytes.TrimSpace(out)) > 0
}

func TestSampleProgramRunsUnderGVisor(t *testing.T) {
	runner, ok := runtimes(t)[RuntimeGVisor]
	if !ok {
		t.Skip("runsc not registered")
	}
	spec := python("import sys\nprint('sum', sum(int(x) for x in sys.stdin.read().split()))\nprint(open('/proc/version').read())\n")
	spec.Files["helper.py"] = []byte("VALUE = 1\n")
	spec.Stdin = []byte("1 2 3 4")
	result := mustRun(t, runner, spec)
	if result.Status != StatusSucceeded || !strings.Contains(string(result.Stdout), "sum 10") {
		t.Fatalf("result = %+v stdout %q stderr %q", result, result.Stdout, result.Stderr)
	}
	// dmesg inside gVisor comes from its user-space kernel, not the host.
	dmesg := mustRun(t, runner, Spec{ExecutionID: "it-" + uuid.NewString()[:12], Language: "shell", Entrypoint: "run.sh",
		Files: map[string][]byte{"run.sh": []byte("dmesg | head -1\n")}})
	if !strings.Contains(string(dmesg.Stdout), "gVisor") {
		t.Errorf("not running under gVisor: %q", dmesg.Stdout)
	}
	if containerExists(t, spec) {
		t.Error("container not removed after the execution")
	}
}

// egressProbe tries public egress, the cloud metadata service and DNS, and
// prints one line per target.
const egressProbe = `
import socket
def probe(name, host, port):
    try:
        socket.create_connection((host, port), timeout=3).close()
        print(name, "OPEN")
    except OSError as e:
        print(name, "blocked", type(e).__name__)
probe("egress", "1.1.1.1", 443)
probe("egress-http", "8.8.8.8", 80)
probe("imds", "169.254.169.254", 80)
try:
    socket.getaddrinfo("example.com", 443)
    print("dns OPEN")
except OSError as e:
    print("dns blocked", type(e).__name__)
import os
print("interfaces", sorted(os.listdir("/sys/class/net")) if os.path.isdir("/sys/class/net") else "n/a")
`

func TestEgressAndIMDSAreBlocked(t *testing.T) {
	for name, runner := range runtimes(t) {
		t.Run(name, func(t *testing.T) {
			result := mustRun(t, runner, python(egressProbe))
			out := string(result.Stdout)
			if result.Status != StatusSucceeded {
				t.Fatalf("probe failed: %+v stderr %q", result, result.Stderr)
			}
			for _, target := range []string{"egress", "egress-http", "imds", "dns"} {
				if !strings.Contains(out, target+" blocked") {
					t.Errorf("%s not blocked:\n%s", target, out)
				}
			}
			if strings.Contains(out, "OPEN") {
				t.Errorf("a connection succeeded:\n%s", out)
			}
		})
	}
}

func TestWallClockTimeoutKillsRealContainer(t *testing.T) {
	for name, runner := range runtimes(t) {
		t.Run(name, func(t *testing.T) {
			spec := python("import time\nprint('started', flush=True)\nwhile True:\n    time.sleep(0.1)\n")
			spec.Limits.WallClock = 3 * time.Second
			started := time.Now()
			result := mustRun(t, runner, spec)
			if result.Status != StatusTimedOut {
				t.Fatalf("status = %s, want timed_out", result.Status)
			}
			if elapsed := time.Since(started); elapsed > 20*time.Second {
				t.Errorf("timeout took %s", elapsed)
			}
			if !strings.Contains(string(result.Stdout), "started") {
				t.Errorf("output before the kill was lost: %q", result.Stdout)
			}
			if containerExists(t, spec) {
				t.Error("timed-out container still exists")
			}
		})
	}
}

const hardeningProbe = `
import os, subprocess
print("uid", os.getuid(), "gid", os.getgid())
for path in ["/pwned", "/workspace/pwned", "/tmp/ok"]:
    try:
        open(path, "w").write("x")
        print("write", path, "allowed")
    except OSError:
        print("write", path, "denied")
open("/tmp/run.sh", "w").write("#!/bin/sh\necho ran\n")
os.chmod("/tmp/run.sh", 0o755)
try:
    subprocess.run(["/tmp/run.sh"], check=True)
    print("exec /tmp allowed")
except OSError:
    print("exec /tmp denied")
status = open("/proc/self/status").read()
for line in status.splitlines():
    if line.split(":")[0] in ("CapEff", "NoNewPrivs", "Seccomp"):
        print(line.replace("\t", " "))
`

func TestHardening(t *testing.T) {
	for name, runner := range runtimes(t) {
		t.Run(name, func(t *testing.T) {
			result := mustRun(t, runner, python(hardeningProbe))
			out := string(result.Stdout)
			for _, want := range []string{
				"uid 65534 gid 65534", "write /pwned denied", "write /workspace/pwned denied",
				"write /tmp/ok allowed", "exec /tmp denied", "CapEff: 0000000000000000",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %q in:\n%s\nstderr: %s", want, out, result.Stderr)
				}
			}
			// gVisor filters syscalls in its own kernel and hides host
			// seccomp; with runc the engine's filter must be active.
			if name == RuntimeRunc {
				for _, want := range []string{"NoNewPrivs: 1", "Seccomp: 2"} {
					if !strings.Contains(out, want) {
						t.Errorf("missing %q in:\n%s", want, out)
					}
				}
			}
		})
	}
}

func TestResourceLimits(t *testing.T) {
	for name, runner := range runtimes(t) {
		t.Run(name, func(t *testing.T) {
			memory := python("chunks = []\nwhile True:\n    chunks.append(bytearray(16 << 20))\n")
			memory.Limits.MemoryBytes = 64 << 20
			result := mustRun(t, runner, memory)
			if result.Status != StatusOOMKilled && result.Status != StatusFailed {
				t.Errorf("memory hog status = %s, want oom_killed or failed", result.Status)
			}
			t.Logf("memory hog: status=%s exit=%d", result.Status, result.ExitCode)

			pids := python("import threading, time\nn = 0\ntry:\n    while n < 500:\n        threading.Thread(target=time.sleep, args=(5,), daemon=True).start()\n        n += 1\nexcept RuntimeError:\n    pass\nprint('threads', n)\n")
			pids.Limits.PIDs = 32
			result = mustRun(t, runner, pids)
			if strings.Contains(string(result.Stdout), "threads 500") {
				t.Errorf("pids limit not enforced: %q", result.Stdout)
			}

			noisy := python("import sys\nsys.stdout.write('x' * (3 << 20))\n")
			noisy.Limits.OutputBytes = 1 << 10
			result = mustRun(t, runner, noisy)
			if len(result.Stdout) != 1<<10 || !result.StdoutTruncated || result.Status != StatusSucceeded {
				t.Errorf("output cap: len=%d truncated=%v status=%s", len(result.Stdout), result.StdoutTruncated, result.Status)
			}
		})
	}
}

func TestReapRemovesLeftovers(t *testing.T) {
	runner := runtimes(t)[RuntimeRunc]
	name := containerPrefix + "reap-" + uuid.NewString()[:8]
	if out, err := exec.Command("docker", "create", "--name", name, "--label", LabelSandbox+"=true", "alpine:3.20", "true").CombinedOutput(); err != nil {
		t.Fatalf("create leftover: %v %s", err, out)
	}
	n, err := runner.Reap(context.Background())
	if err != nil || n < 1 {
		t.Fatalf("Reap = %d, %v", n, err)
	}
	if out, _ := exec.Command("docker", "ps", "-aq", "--filter", "name="+name).Output(); len(bytes.TrimSpace(out)) != 0 {
		t.Error("leftover container not reaped")
	}
}
