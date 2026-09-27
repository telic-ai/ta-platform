package candidateworkspace

import (
	"strings"
	"testing"
)

func TestCountUnifiedDiff(t *testing.T) {
	cases := []struct {
		name           string
		patch          string
		added, removed int
	}{
		{"git style with headers", "diff --git a/main.py b/main.py\nindex 1..2 100644\n--- a/main.py\n+++ b/main.py\n@@ -1,3 +1,4 @@\n import sys\n-print(1)\n+print(2)\n+print(3)\n x = 1\n", 2, 1},
		{"bare hunk", "@@ -1 +1 @@\n-a\n+b\n", 1, 1},
		{"pure addition to empty file", "--- /dev/null\n+++ b/new.py\n@@ -0,0 +1,2 @@\n+one\n+two\n", 2, 0},
		{"deletion", "@@ -1,2 +0,0 @@\n-one\n-two\n", 0, 2},
		{"two hunks", "@@ -1,2 +1,2 @@\n-a\n+A\n b\n@@ -10,1 +10,2 @@\n c\n+d\n", 2, 1},
		{"no newline marker", "@@ -1 +1 @@\n-a\n\\ No newline at end of file\n+b\n\\ No newline at end of file\n", 1, 1},
		{"empty context line", "@@ -1,3 +1,3 @@\n a\n\n-c\n+C\n", 1, 1},
		{"lines that look like headers inside a hunk", "@@ -1,2 +1,2 @@\n---x\n++++y\n-- z\n++ w\n", 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			added, removed, err := countUnifiedDiff(tc.patch)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if added != tc.added || removed != tc.removed {
				t.Errorf("got +%d -%d, want +%d -%d", added, removed, tc.added, tc.removed)
			}
		})
	}
}

func TestCountUnifiedDiffRejects(t *testing.T) {
	for name, patch := range map[string]string{
		"no hunks":           "--- a/x\n+++ b/x\n",
		"garbage":            "hello world\n",
		"bad header":         "@@ -a +b @@\n+x\n",
		"count mismatch":     "@@ -1,2 +1,2 @@\n-a\n+b\n",
		"truncated hunk":     "@@ -1,3 +1,3 @@\n a\n",
		"stray line in hunk": "@@ -1 +1 @@\n-a\n+b\n?what\n",
	} {
		if _, _, err := countUnifiedDiff(patch); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(truncate(strings.Repeat("x", 100))) > 45 {
		t.Error("truncate did not shorten")
	}
}
