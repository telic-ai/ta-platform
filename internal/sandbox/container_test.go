package sandbox

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCLI scripts container-engine responses per subcommand and records
// every call.
type fakeCLI struct {
	mu    sync.Mutex
	calls [][]string
	// mountDir captures the snapshot dir seen at create time.
	mountDir string
	files    map[string]string
	stdin    string

	createErr error
	stdout    string
	stderr    string
	startErr  error
	startWait bool // block start until ctx is done
	state     string
	killErr   error
	psOut     string
}

func (f *fakeCLI) Run(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) error {
	f.mu.Lock()
	f.calls = append(f.calls, args)
	f.mu.Unlock()
	switch args[0] {
	case "create":
		for i, arg := range args {
			if arg == "--mount" {
				spec := args[i+1]
				f.mountDir = strings.TrimPrefix(strings.Split(spec, ",")[1], "source=")
				f.files = map[string]string{}
				_ = filepath.Walk(f.mountDir, func(path string, info os.FileInfo, err error) error {
					if err == nil && !info.IsDir() {
						rel, _ := filepath.Rel(f.mountDir, path)
						content, _ := os.ReadFile(path)
						f.files[filepath.ToSlash(rel)] = string(content)
					}
					return nil
				})
			}
		}
		return f.createErr
	case "start":
		data, _ := io.ReadAll(stdin)
		f.stdin = string(data)
		_, _ = io.WriteString(stdout, f.stdout)
		_, _ = io.WriteString(stderr, f.stderr)
		if f.startWait {
			<-ctx.Done()
			return ctx.Err()
		}
		return f.startErr
	case "inspect":
		if f.state == "" {
			return errors.New("no such container")
		}
		_, _ = io.WriteString(stdout, f.state)
		return nil
	case "kill":
		return f.killErr
	case "ps":
		_, _ = io.WriteString(stdout, f.psOut)
	}
	return nil
}

func (f *fakeCLI) subcommands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, call := range f.calls {
		out = append(out, call[0])
	}
	return out
}

func (f *fakeCLI) call(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, call := range f.calls {
		if call[0] == name {
			return call
		}
	}
	return nil
}

func pySpec() Spec {
	return Spec{ExecutionID: "exec-1", Language: "python", Entrypoint: "main.py",
		Files: map[string][]byte{"main.py": []byte("print(1)"), "lib/util.py": []byte("X = 1")}, Stdin: []byte("in")}
}

// flagValues returns every value following flag in args.
func flagValues(args []string, flag string) []string {
	var out []string
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			out = append(out, args[i+1])
		}
	}
	return out
}

func TestRunnersUseTheirRuntimeAndSatisfyRunner(t *testing.T) {
	for want, runner := range map[string]Runner{
		RuntimeGVisor: NewGVisorRunner(ContainerConfig{CLI: &fakeCLI{}}),
		RuntimeRunc:   NewDockerRunner(ContainerConfig{CLI: &fakeCLI{}}),
	} {
		cli := runner.(*ContainerRunner).cli.(*fakeCLI)
		cli.state = `{"Running":false,"OOMKilled":false,"ExitCode":0}`
		if _, err := runner.Run(context.Background(), pySpec()); err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		if got := flagValues(cli.call("create"), "--runtime"); !slices.Equal(got, []string{want}) {
			t.Errorf("runtime = %v, want %s", got, want)
		}
	}
}

func TestCreateAppliesHardening(t *testing.T) {
	cli := &fakeCLI{state: `{"ExitCode":0}`}
	runner := NewGVisorRunner(ContainerConfig{CLI: cli})
	spec := pySpec()
	spec.Limits = Limits{WallClock: 5 * time.Second, CPUs: 0.5, MemoryBytes: 128 << 20, PIDs: 32, TmpBytes: 16 << 20}
	if _, err := runner.Run(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	args := cli.call("create")
	joined := strings.Join(args, " ")
	for flag, want := range map[string]string{
		"--network": "none", "--user": NobodyUser, "--cap-drop": "ALL", "--memory": "134217728",
		"--memory-swap": "134217728", "--cpus": "0.5", "--pids-limit": "32", "--pull": "never",
		"--log-driver": "none", "--name": "ta-sbx-exec-1", "--workdir": "/workspace",
	} {
		if got := flagValues(args, flag); !slices.Equal(got, []string{want}) {
			t.Errorf("%s = %v, want %q", flag, got, want)
		}
	}
	for _, want := range []string{"--read-only", "no-new-privileges", "seccomp=builtin", ",readonly",
		"/tmp:rw,noexec,nosuid,nodev,size=16777216", "python:3.12-alpine python3 -I -B main.py"} {
		if !strings.Contains(joined, want) {
			t.Errorf("create args missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "unconfined") || strings.Contains(joined, "169.254") {
		t.Errorf("create args weaken isolation: %s", joined)
	}
	for _, forbidden := range []string{"--privileged", "--cap-add", "--pid", "--ipc", "--userns", "--add-host", "--device", "--volume"} {
		if slices.Contains(args, forbidden) {
			t.Errorf("create args contain %q", forbidden)
		}
	}
}

func TestProcessLimitPerRuntime(t *testing.T) {
	spec := pySpec()
	spec.Limits.PIDs = 40
	for runtime, runner := range map[string]*ContainerRunner{
		RuntimeGVisor: NewGVisorRunner(ContainerConfig{CLI: &fakeCLI{state: `{"ExitCode":0}`}}),
		RuntimeRunc:   NewDockerRunner(ContainerConfig{CLI: &fakeCLI{state: `{"ExitCode":0}`}}),
	} {
		if _, err := runner.Run(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
		args := runner.cli.(*fakeCLI).call("create")
		if !slices.Equal(flagValues(args, "--pids-limit"), []string{"40"}) {
			t.Errorf("%s: pids-limit = %v", runtime, flagValues(args, "--pids-limit"))
		}
		hasNproc := slices.Contains(flagValues(args, "--ulimit"), "nproc=40:40")
		if hasNproc != (runtime == RuntimeGVisor) {
			t.Errorf("%s: nproc ulimit present = %v", runtime, hasNproc)
		}
	}
}

func TestCustomSeccompProfile(t *testing.T) {
	cli := &fakeCLI{state: `{"ExitCode":0}`}
	if _, err := NewDockerRunner(ContainerConfig{CLI: cli, SeccompProfile: "/etc/sbx.json"}).Run(context.Background(), pySpec()); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(flagValues(cli.call("create"), "--security-opt"), "seccomp=/etc/sbx.json") {
		t.Errorf("security opts = %v", flagValues(cli.call("create"), "--security-opt"))
	}
}

func TestRunLifecycleAndSnapshot(t *testing.T) {
	cli := &fakeCLI{stdout: "out", stderr: "err", state: `{"Running":false,"OOMKilled":false,"ExitCode":0}`}
	result, err := NewGVisorRunner(ContainerConfig{CLI: cli, WorkDir: t.TempDir()}).Run(context.Background(), pySpec())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cli.subcommands(), ","); got != "create,start,inspect,rm" {
		t.Errorf("lifecycle = %s", got)
	}
	if result.Status != StatusSucceeded || string(result.Stdout) != "out" || string(result.Stderr) != "err" || cli.stdin != "in" {
		t.Errorf("result = %+v stdin %q", result, cli.stdin)
	}
	if cli.files["main.py"] != "print(1)" || cli.files["lib/util.py"] != "X = 1" {
		t.Errorf("snapshot = %v", cli.files)
	}
	if _, err := os.Stat(cli.mountDir); !os.IsNotExist(err) {
		t.Errorf("snapshot dir %s not removed", cli.mountDir)
	}
}

func TestRunStatuses(t *testing.T) {
	for state, want := range map[string]Status{
		`{"ExitCode":0}`:                    StatusSucceeded,
		`{"ExitCode":1}`:                    StatusFailed,
		`{"ExitCode":137,"OOMKilled":true}`: StatusOOMKilled,
	} {
		cli := &fakeCLI{state: state}
		result, err := NewDockerRunner(ContainerConfig{CLI: cli}).Run(context.Background(), pySpec())
		if err != nil || result.Status != want {
			t.Errorf("%s: status %s err %v, want %s", state, result.Status, err, want)
		}
	}
}

func TestWallClockTimeoutKillsContainer(t *testing.T) {
	cli := &fakeCLI{startWait: true, stdout: "partial"}
	spec := pySpec()
	spec.Limits.WallClock = 50 * time.Millisecond
	result, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Run(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusTimedOut || string(result.Stdout) != "partial" {
		t.Errorf("result = %+v", result)
	}
	if got := strings.Join(cli.subcommands(), ","); got != "create,start,kill,rm" {
		t.Errorf("lifecycle = %s, want kill then rm", got)
	}
	if got := cli.call("kill"); !slices.Equal(got, []string{"kill", "--signal", "KILL", "ta-sbx-exec-1"}) {
		t.Errorf("kill = %v", got)
	}
}

func TestCallerCancellationKillsAndReturnsError(t *testing.T) {
	cli := &fakeCLI{startWait: true}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Run(ctx, pySpec())
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	if subs := cli.subcommands(); !slices.Contains(subs, "kill") || subs[len(subs)-1] != "rm" {
		t.Errorf("lifecycle = %v", subs)
	}
}

func TestFailedKillOfRunningContainerIsAnError(t *testing.T) {
	cli := &fakeCLI{startWait: true, killErr: errors.New("daemon gone"), state: `{"Running":true}`}
	spec := pySpec()
	spec.Limits.WallClock = 20 * time.Millisecond
	if _, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Run(context.Background(), spec); err == nil {
		t.Error("a container that could not be killed was reported as timed out")
	}
}

func TestCreateFailureStillRemoves(t *testing.T) {
	cli := &fakeCLI{createErr: errors.New("no such image")}
	if _, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Run(context.Background(), pySpec()); err == nil {
		t.Fatal("create failure not reported")
	}
	if got := strings.Join(cli.subcommands(), ","); got != "create,rm" {
		t.Errorf("lifecycle = %s", got)
	}
}

func TestInvalidSpecRunsNothing(t *testing.T) {
	cli := &fakeCLI{}
	spec := pySpec()
	spec.Language = "cobol"
	if _, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Run(context.Background(), spec); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("err = %v", err)
	}
	if len(cli.calls) != 0 {
		t.Errorf("calls = %v", cli.calls)
	}
}

func TestReap(t *testing.T) {
	cli := &fakeCLI{psOut: "abc\ndef\n"}
	n, err := NewGVisorRunner(ContainerConfig{CLI: cli}).Reap(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("Reap = %d, %v", n, err)
	}
	if got := cli.call("rm"); !slices.Equal(got, []string{"rm", "--force", "--volumes", "abc", "def"}) {
		t.Errorf("rm = %v", got)
	}
	if !slices.Contains(cli.call("ps"), "label="+LabelSandbox+"=true") {
		t.Errorf("ps = %v", cli.call("ps"))
	}
	empty := &fakeCLI{}
	if n, err := NewGVisorRunner(ContainerConfig{CLI: empty}).Reap(context.Background()); n != 0 || err != nil || empty.call("rm") != nil {
		t.Errorf("empty reap = %d, %v, %v", n, err, empty.calls)
	}
}

func TestLimitedBuffer(t *testing.T) {
	b := &limitedBuffer{limit: 5}
	for _, chunk := range []string{"abc", "defg", "h"} {
		if n, err := b.Write([]byte(chunk)); n != len(chunk) || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	if b.buf.String() != "abcde" || !b.truncated {
		t.Errorf("buf %q truncated %v", b.buf.String(), b.truncated)
	}
	exact := &limitedBuffer{limit: 3}
	_, _ = exact.Write([]byte("abc"))
	if exact.truncated {
		t.Error("exactly-full buffer marked truncated")
	}
}

func TestExitCode(t *testing.T) {
	if exitCode(errors.New("x")) != -1 {
		t.Error("non-exit error")
	}
	err := ExecCLI{Binary: "sh"}.Run(context.Background(), nil, io.Discard, io.Discard, "-c", "exit 7")
	if exitCode(err) != 7 {
		t.Errorf("exitCode = %d", exitCode(err))
	}
}
