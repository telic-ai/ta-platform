package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// RuntimeGVisor is gVisor's OCI runtime (ADR-004).
	RuntimeGVisor = "runsc"
	// RuntimeRunc is the default container runtime, for local development.
	RuntimeRunc = "runc"

	// LabelSandbox marks every sandbox container so leftovers can be reaped.
	LabelSandbox = "ta-platform.sandbox"
	// NobodyUser is the unprivileged uid:gid code runs as.
	NobodyUser = "65534:65534"

	containerPrefix = "ta-sbx-"
	workspaceMount  = "/workspace"
	cleanupTimeout  = 15 * time.Second
)

// CLI runs container-engine commands (docker, or a compatible CLI).
type CLI interface {
	Run(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) error
}

// ExecCLI runs a container CLI binary.
type ExecCLI struct{ Binary string }

func (c ExecCLI) Run(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) error {
	binary := c.Binary
	if binary == "" {
		binary = "docker"
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	// If the CLI is killed, don't wait forever for its output pipes.
	cmd.WaitDelay = 2 * time.Second
	return cmd.Run()
}

// exitCode extracts a CLI exit code, or -1.
func exitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

// ContainerConfig configures a ContainerRunner.
type ContainerConfig struct {
	// CLI defaults to ExecCLI{"docker"}.
	CLI CLI
	// SeccompProfile is a path to a seccomp JSON profile. Empty means the
	// engine's default profile, which is always applied (never unconfined).
	SeccompProfile string
	// WorkDir is where per-execution snapshot directories are created;
	// empty means os.TempDir().
	WorkDir string
}

// ContainerRunner runs each execution in its own container.
type ContainerRunner struct {
	cli     CLI
	runtime string
	config  ContainerConfig
	now     func() time.Time
}

// NewGVisorRunner returns a runner that uses the gVisor runtime. The
// engine must have runsc registered as a runtime.
func NewGVisorRunner(config ContainerConfig) *ContainerRunner {
	return newContainerRunner(RuntimeGVisor, config)
}

// NewDockerRunner returns a runner on the engine's default runc runtime:
// the same hardening without gVisor's kernel isolation. Local dev only.
func NewDockerRunner(config ContainerConfig) *ContainerRunner {
	return newContainerRunner(RuntimeRunc, config)
}

func newContainerRunner(runtime string, config ContainerConfig) *ContainerRunner {
	cli := config.CLI
	if cli == nil {
		cli = ExecCLI{}
	}
	return &ContainerRunner{cli: cli, runtime: runtime, config: config, now: time.Now}
}

// Runtime reports the OCI runtime this runner uses.
func (r *ContainerRunner) Runtime() string { return r.runtime }

// Run creates, runs, inspects and removes one container.
func (r *ContainerRunner) Run(ctx context.Context, spec Spec) (Result, error) {
	spec, language, err := normalize(spec)
	if err != nil {
		return Result{}, err
	}
	dir, err := writeSnapshot(r.config.WorkDir, spec)
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(dir)

	name := containerPrefix + spec.ExecutionID
	// Remove the container however Run ends, even if ctx is cancelled.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_ = r.cli.Run(cleanupCtx, nil, io.Discard, io.Discard, "rm", "--force", "--volumes", name)
	}()

	var createErr bytes.Buffer
	if err := r.cli.Run(ctx, nil, io.Discard, &createErr, r.createArgs(name, dir, spec, language)...); err != nil {
		return Result{}, fmt.Errorf("sandbox: create container: %w: %s", err, strings.TrimSpace(createErr.String()))
	}

	stdout := &limitedBuffer{limit: spec.Limits.OutputBytes}
	stderr := &limitedBuffer{limit: spec.Limits.OutputBytes}
	runCtx, cancel := context.WithTimeout(ctx, spec.Limits.WallClock)
	defer cancel()
	started := r.now()
	startErr := r.cli.Run(runCtx, bytes.NewReader(spec.Stdin), stdout, stderr, "start", "--attach", "--interactive", name)
	result := Result{
		Duration: r.now().Sub(started),
		Stdout:   stdout.buf.Bytes(), StdoutTruncated: stdout.truncated,
		Stderr: stderr.buf.Bytes(), StderrTruncated: stderr.truncated,
	}

	if runCtx.Err() != nil {
		// The CLI was killed, not the container: kill it explicitly.
		killCtx, cancelKill := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		killErr := r.cli.Run(killCtx, nil, io.Discard, io.Discard, "kill", "--signal", "KILL", name)
		cancelKill()
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if killErr != nil && !r.stopped(ctx, name) {
			return result, fmt.Errorf("sandbox: kill timed-out container: %w", killErr)
		}
		result.Status, result.ExitCode = StatusTimedOut, -1
		return result, nil
	}

	state, err := r.inspect(ctx, name)
	if err != nil {
		return result, err
	}
	if state.Running {
		return result, fmt.Errorf("sandbox: container still running after attach ended: %v", startErr)
	}
	result.ExitCode = state.ExitCode
	switch {
	case state.OOMKilled:
		result.Status = StatusOOMKilled
	case state.ExitCode == 0:
		result.Status = StatusSucceeded
	default:
		result.Status = StatusFailed
	}
	return result, nil
}

func (r *ContainerRunner) createArgs(name, dir string, spec Spec, language Language) []string {
	limits := spec.Limits
	seccomp := "seccomp=builtin"
	if r.config.SeccompProfile != "" {
		seccomp = "seccomp=" + r.config.SeccompProfile
	}
	args := []string{
		"create",
		"--name", name,
		"--runtime", r.runtime,
		"--pull", "never",
		"--label", LabelSandbox + "=true",
		"--label", LabelSandbox + ".execution-id=" + spec.ExecutionID,
		// Deny-all egress: no interfaces but loopback, so no route to the
		// internet or to the metadata service (169.254.169.254).
		"--network", "none",
		"--hostname", "sandbox",
		"--read-only",
		"--tmpfs", fmt.Sprintf("/tmp:rw,noexec,nosuid,nodev,size=%d,mode=1777", limits.TmpBytes),
		"--mount", "type=bind,source=" + dir + ",target=" + workspaceMount + ",readonly",
		"--workdir", workspaceMount,
		"--user", NobodyUser,
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"--security-opt", seccomp,
		"--memory", strconv.FormatInt(limits.MemoryBytes, 10),
		"--memory-swap", strconv.FormatInt(limits.MemoryBytes, 10),
		"--cpus", strconv.FormatFloat(limits.CPUs, 'f', -1, 64),
		"--pids-limit", strconv.FormatInt(limits.PIDs, 10),
		"--ulimit", "nofile=256:256",
		"--ulimit", fmt.Sprintf("fsize=%d:%d", limits.TmpBytes, limits.TmpBytes),
		// Candidate output is returned in the Result, never kept in engine logs.
		"--log-driver", "none",
		"--env", "HOME=/tmp",
		"--interactive",
	}
	if r.runtime == RuntimeGVisor {
		// Guest threads and processes are tasks inside gVisor's kernel, not
		// host PIDs, so --pids-limit does not bound them. gVisor enforces
		// RLIMIT_NPROC in its own per-sandbox uid space. (Under runc, nproc
		// counts every sandbox sharing the uid, so it is not set there.)
		args = append(args, "--ulimit", fmt.Sprintf("nproc=%d:%d", limits.PIDs, limits.PIDs))
	}
	args = append(args, language.Image)
	return append(args, language.Command(spec.Entrypoint)...)
}

type containerState struct {
	Running   bool `json:"Running"`
	OOMKilled bool `json:"OOMKilled"`
	ExitCode  int  `json:"ExitCode"`
}

func (r *ContainerRunner) inspect(ctx context.Context, name string) (containerState, error) {
	var out, stderr bytes.Buffer
	if err := r.cli.Run(ctx, nil, &out, &stderr, "inspect", "--format", "{{json .State}}", name); err != nil {
		return containerState{}, fmt.Errorf("sandbox: inspect container: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var state containerState
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &state); err != nil {
		return containerState{}, fmt.Errorf("sandbox: decode container state: %w", err)
	}
	return state, nil
}

// stopped reports whether the container has exited (or no longer exists).
func (r *ContainerRunner) stopped(ctx context.Context, name string) bool {
	state, err := r.inspect(context.WithoutCancel(ctx), name)
	return err != nil || !state.Running
}

// Reap removes sandbox containers left behind by a crashed process. Call it
// at startup, before accepting executions.
func (r *ContainerRunner) Reap(ctx context.Context) (int, error) {
	var out bytes.Buffer
	if err := r.cli.Run(ctx, nil, &out, io.Discard, "ps", "--all", "--quiet", "--filter", "label="+LabelSandbox+"=true"); err != nil {
		return 0, fmt.Errorf("sandbox: list leftover containers: %w", err)
	}
	ids := strings.Fields(out.String())
	if len(ids) == 0 {
		return 0, nil
	}
	if err := r.cli.Run(ctx, nil, io.Discard, io.Discard, append([]string{"rm", "--force", "--volumes"}, ids...)...); err != nil {
		return 0, fmt.Errorf("sandbox: remove leftover containers: %w", err)
	}
	return len(ids), nil
}

// writeSnapshot materializes the files in a fresh directory that the
// container mounts read-only.
func writeSnapshot(parent string, spec Spec) (string, error) {
	dir, err := os.MkdirTemp(parent, "ta-sbx-"+spec.ExecutionID+"-")
	if err != nil {
		return "", fmt.Errorf("sandbox: create snapshot dir: %w", err)
	}
	// The container user is not the owner; it needs read and traverse.
	if err := os.Chmod(dir, 0o755); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("sandbox: chmod snapshot dir: %w", err)
	}
	for name, content := range spec.Files {
		target := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("sandbox: create %s: %w", name, err)
		}
		if err := os.WriteFile(target, content, 0o644); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("sandbox: write %s: %w", name, err)
		}
	}
	return dir, nil
}

// limitedBuffer keeps the first limit bytes and discards the rest, so a
// noisy program can neither exhaust memory nor block on a full pipe.
type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.buf.Len(); room > 0 {
		if len(p) > room {
			b.buf.Write(p[:room])
			b.truncated = true
		} else {
			b.buf.Write(p)
		}
	} else if len(p) > 0 {
		b.truncated = true
	}
	return len(p), nil
}
