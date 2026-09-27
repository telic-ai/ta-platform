package candidateworkspace

import (
	"encoding/json"
	"os"
	"testing"
)

// TestBrowserPatchesAreAccepted checks every patch the candidate app's
// makePatch produces (web/packages/api-client/test/fixtures/patches.json,
// kept current by that package's contract test) is accepted with the
// expected line counts.
func TestBrowserPatchesAreAccepted(t *testing.T) {
	raw, err := os.ReadFile("../../web/packages/api-client/test/fixtures/patches.json")
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	var fixtures []struct {
		Name         string `json:"name"`
		Path         string `json:"path"`
		Patch        string `json:"patch"`
		LinesAdded   int    `json:"linesAdded"`
		LinesRemoved int    `json:"linesRemoved"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) == 0 {
		t.Fatal("no fixtures")
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			added, removed, err := validateDiff(DiffRequest{ClientSequence: 1, Origin: "manual", Path: fixture.Path, Patch: fixture.Patch})
			if err != nil {
				t.Fatalf("rejected: %v\n%s", err, fixture.Patch)
			}
			if added != fixture.LinesAdded || removed != fixture.LinesRemoved {
				t.Errorf("+%d -%d, want +%d -%d", added, removed, fixture.LinesAdded, fixture.LinesRemoved)
			}
		})
	}
}
