package candidateworkspace

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var hunkHeader = regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)

// countUnifiedDiff validates a single-file unified diff and counts its
// added and removed lines. Each hunk's body must match the line counts in
// its header, so a truncated or hand-mangled patch is rejected.
func countUnifiedDiff(patch string) (added, removed int, err error) {
	lines := strings.Split(strings.TrimSuffix(patch, "\n"), "\n")
	i := 0
	// Optional file headers before the first hunk.
	for i < len(lines) && !strings.HasPrefix(lines[i], "@@") {
		line := lines[i]
		if !strings.HasPrefix(line, "--- ") && !strings.HasPrefix(line, "+++ ") &&
			!strings.HasPrefix(line, "diff ") && !strings.HasPrefix(line, "index ") {
			return 0, 0, fmt.Errorf("line %d: unexpected %q before the first hunk", i+1, truncate(line))
		}
		i++
	}
	hunks := 0
	for i < len(lines) {
		match := hunkHeader.FindStringSubmatch(lines[i])
		if match == nil {
			return 0, 0, fmt.Errorf("line %d: expected a hunk header, got %q", i+1, truncate(lines[i]))
		}
		oldCount, newCount := hunkLength(match[2]), hunkLength(match[4])
		hunks++
		i++
		oldSeen, newSeen := 0, 0
		for i < len(lines) && !strings.HasPrefix(lines[i], "@@") {
			line := lines[i]
			switch {
			case strings.HasPrefix(line, "+"):
				added++
				newSeen++
			case strings.HasPrefix(line, "-"):
				removed++
				oldSeen++
			case strings.HasPrefix(line, " ") || line == "":
				oldSeen++
				newSeen++
			case strings.HasPrefix(line, `\`):
				// "\ No newline at end of file"
			default:
				return 0, 0, fmt.Errorf("line %d: unexpected %q in hunk", i+1, truncate(line))
			}
			i++
		}
		if oldSeen != oldCount || newSeen != newCount {
			return 0, 0, fmt.Errorf("hunk %d: header says -%d +%d, body has -%d +%d", hunks, oldCount, newCount, oldSeen, newSeen)
		}
	}
	if hunks == 0 {
		return 0, 0, errors.New("patch has no hunks")
	}
	return added, removed, nil
}

func hunkLength(field string) int {
	if field == "" {
		return 1
	}
	n, _ := strconv.Atoi(field)
	return n
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "…"
	}
	return s
}
