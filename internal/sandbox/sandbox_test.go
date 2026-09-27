package sandbox

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNormalizeFillsDefaults(t *testing.T) {
	spec, language, err := normalize(pySpec())
	if err != nil {
		t.Fatal(err)
	}
	if spec.Limits != DefaultLimits || language.Image != "python:3.12-alpine" {
		t.Errorf("limits %+v image %s", spec.Limits, language.Image)
	}
	partial := pySpec()
	partial.Limits = Limits{WallClock: 3 * time.Second}
	spec, _, _ = normalize(partial)
	if spec.Limits.WallClock != 3*time.Second || spec.Limits.MemoryBytes != DefaultLimits.MemoryBytes {
		t.Errorf("limits = %+v", spec.Limits)
	}
}

func TestNormalizeRejects(t *testing.T) {
	cases := map[string]func(*Spec){
		"empty id":          func(s *Spec) { s.ExecutionID = "" },
		"id with slash":     func(s *Spec) { s.ExecutionID = "a/b" },
		"id too long":       func(s *Spec) { s.ExecutionID = strings.Repeat("a", 64) },
		"language":          func(s *Spec) { s.Language = "ruby" },
		"no files":          func(s *Spec) { s.Files = nil },
		"missing entry":     func(s *Spec) { s.Entrypoint = "nope.py" },
		"absolute path":     func(s *Spec) { s.Files["/etc/passwd"] = nil },
		"parent escape":     func(s *Spec) { s.Files["../x"] = nil },
		"unclean path":      func(s *Spec) { s.Files["a/../b"] = nil },
		"backslash":         func(s *Spec) { s.Files[`a\b`] = nil },
		"comma (mount opt)": func(s *Spec) { s.Files["a,b"] = nil },
		"too big":           func(s *Spec) { s.Files["big"] = make([]byte, maxSnapshotBytes+1) },
		"wall clock":        func(s *Spec) { s.Limits.WallClock = time.Hour },
		"memory":            func(s *Spec) { s.Limits.MemoryBytes = 8 << 30 },
		"cpus":              func(s *Spec) { s.Limits.CPUs = 16 },
		"pids":              func(s *Spec) { s.Limits.PIDs = 100000 },
		"output":            func(s *Spec) { s.Limits.OutputBytes = 1 << 30 },
		"tmp":               func(s *Spec) { s.Limits.TmpBytes = 1 << 40 },
	}
	for name, breakIt := range cases {
		spec := pySpec()
		breakIt(&spec)
		if _, _, err := normalize(spec); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s: err = %v, want ErrInvalidSpec", name, err)
		}
	}
}

func TestLanguagesHaveCommands(t *testing.T) {
	for name, language := range Languages {
		if language.Image == "" || len(language.Command("main")) == 0 || !strings.Contains(strings.Join(language.Command("main"), " "), "main") {
			t.Errorf("language %s is incomplete", name)
		}
	}
}
