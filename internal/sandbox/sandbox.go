// Package sandbox runs untrusted candidate code in a locked-down,
// per-execution container.
//
// Every execution gets a fresh container that is created, run, inspected
// and removed; nothing is reused between runs (ADR-005). The container has
// no network (deny-all egress, which also blocks the cloud metadata
// service at 169.254.169.254), a read-only root filesystem, a read-only
// /workspace holding the code snapshot, a small noexec /tmp, a non-root
// user, no capabilities, no-new-privileges, a seccomp filter, and CPU,
// memory, PID and wall-clock limits.
//
// Two runners implement Runner: the gVisor runner (runtime runsc, ADR-004)
// and a plain Docker runner (runc) for local development. Both use the
// same hardening flags; gVisor adds a user-space kernel between the code
// and the host.
//
// NOTE: [[Execution Sandbox – Internals]], ADR-004 and ADR-005 were not
// available when this package was written. Limits and statuses below are
// placeholders to reconcile against them.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
)

// Status is the outcome of an execution.
type Status string

const (
	// StatusSucceeded: the program exited 0.
	StatusSucceeded Status = "succeeded"
	// StatusFailed: the program exited non-zero.
	StatusFailed Status = "failed"
	// StatusTimedOut: the wall-clock limit was hit and the container killed.
	StatusTimedOut Status = "timed_out"
	// StatusOOMKilled: the program exceeded its memory limit.
	StatusOOMKilled Status = "oom_killed"
)

// Limits bound one execution.
type Limits struct {
	WallClock   time.Duration
	CPUs        float64
	MemoryBytes int64
	PIDs        int64
	// OutputBytes caps each of stdout and stderr; the rest is discarded.
	OutputBytes int
	// TmpBytes sizes the writable /tmp tmpfs.
	TmpBytes int64
}

// DefaultLimits are conservative MVP limits.
var DefaultLimits = Limits{
	WallClock:   10 * time.Second,
	CPUs:        1,
	MemoryBytes: 256 << 20,
	PIDs:        64,
	OutputBytes: 1 << 20,
	TmpBytes:    64 << 20,
}

// MaxLimits caps what a request may ask for.
var MaxLimits = Limits{
	WallClock:   60 * time.Second,
	CPUs:        2,
	MemoryBytes: 1 << 30,
	PIDs:        256,
	OutputBytes: 4 << 20,
	TmpBytes:    256 << 20,
}

// Spec describes one execution.
type Spec struct {
	// ExecutionID names the container; it must be unique per execution.
	ExecutionID string
	// Language selects the image and default command (see Languages).
	Language string
	// Entrypoint is the snapshot file to run, e.g. "main.py".
	Entrypoint string
	// Files is the code snapshot, keyed by relative path.
	Files map[string][]byte
	Stdin []byte
	// Limits of zero take DefaultLimits' values.
	Limits Limits
}

// Result is what an execution produced.
type Result struct {
	Status          Status
	ExitCode        int
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool
	Duration        time.Duration
}

// Runner runs one execution to completion. An error means the sandbox
// itself failed (bad spec, runtime unavailable), not the candidate's code:
// a crash, non-zero exit, timeout or OOM is a Result.
type Runner interface {
	Run(ctx context.Context, spec Spec) (Result, error)
}

// Language maps a language to its image and command.
type Language struct {
	Image   string
	Command func(entrypoint string) []string
}

// Languages are the supported runtimes. Images must already be pulled on
// the host; the sandbox never pulls (it has no network, and neither should
// execution latency depend on a registry).
var Languages = map[string]Language{
	"python": {Image: "python:3.12-alpine", Command: func(e string) []string {
		return []string{"python3", "-I", "-B", e}
	}},
	"javascript": {Image: "node:22-alpine", Command: func(e string) []string {
		return []string{"node", e}
	}},
	"shell": {Image: "alpine:3.20", Command: func(e string) []string {
		return []string{"/bin/sh", e}
	}},
}

// ErrInvalidSpec is returned for specs rejected before anything runs.
var ErrInvalidSpec = errors.New("sandbox: invalid spec")

const (
	maxFiles         = 200
	maxSnapshotBytes = 8 << 20
)

var executionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// normalize validates spec and fills default limits.
func normalize(spec Spec) (Spec, Language, error) {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, args...))
	}
	if !executionIDPattern.MatchString(spec.ExecutionID) {
		return spec, Language{}, invalid("execution id %q", spec.ExecutionID)
	}
	language, ok := Languages[spec.Language]
	if !ok {
		return spec, Language{}, invalid("unsupported language %q", spec.Language)
	}
	if len(spec.Files) == 0 || len(spec.Files) > maxFiles {
		return spec, Language{}, invalid("snapshot must have 1..%d files", maxFiles)
	}
	total := 0
	for name, content := range spec.Files {
		if err := validPath(name); err != nil {
			return spec, Language{}, invalid("file %q: %v", name, err)
		}
		total += len(content)
	}
	if total > maxSnapshotBytes {
		return spec, Language{}, invalid("snapshot exceeds %d bytes", maxSnapshotBytes)
	}
	if _, ok := spec.Files[spec.Entrypoint]; !ok {
		return spec, Language{}, invalid("entrypoint %q is not in the snapshot", spec.Entrypoint)
	}
	spec.Limits = withDefaults(spec.Limits)
	if err := spec.Limits.within(MaxLimits); err != nil {
		return spec, Language{}, invalid("%v", err)
	}
	return spec, language, nil
}

func validPath(name string) error {
	switch {
	case name == "" || len(name) > 255:
		return errors.New("bad length")
	case strings.HasPrefix(name, "/") || strings.Contains(name, `\`):
		return errors.New("must be relative")
	case path.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") || name == "..":
		return errors.New("must be clean and inside the workspace")
	case strings.ContainsAny(name, "\x00:,"):
		return errors.New("contains a forbidden character")
	}
	return nil
}

func withDefaults(l Limits) Limits {
	if l.WallClock <= 0 {
		l.WallClock = DefaultLimits.WallClock
	}
	if l.CPUs <= 0 {
		l.CPUs = DefaultLimits.CPUs
	}
	if l.MemoryBytes <= 0 {
		l.MemoryBytes = DefaultLimits.MemoryBytes
	}
	if l.PIDs <= 0 {
		l.PIDs = DefaultLimits.PIDs
	}
	if l.OutputBytes <= 0 {
		l.OutputBytes = DefaultLimits.OutputBytes
	}
	if l.TmpBytes <= 0 {
		l.TmpBytes = DefaultLimits.TmpBytes
	}
	return l
}

func (l Limits) within(max Limits) error {
	switch {
	case l.WallClock > max.WallClock:
		return fmt.Errorf("wall clock above %s", max.WallClock)
	case l.CPUs > max.CPUs:
		return fmt.Errorf("cpus above %g", max.CPUs)
	case l.MemoryBytes > max.MemoryBytes:
		return fmt.Errorf("memory above %d bytes", max.MemoryBytes)
	case l.PIDs > max.PIDs:
		return fmt.Errorf("pids above %d", max.PIDs)
	case l.OutputBytes > max.OutputBytes:
		return fmt.Errorf("output above %d bytes", max.OutputBytes)
	case l.TmpBytes > max.TmpBytes:
		return fmt.Errorf("tmp above %d bytes", max.TmpBytes)
	}
	return nil
}
