package eventlogwriter

import (
	"strings"
	"testing"
)

func TestSchemaUsesReplacingMergeTreeDeduplicationKey(t *testing.T) {
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS events",
		"ENGINE = ReplacingMergeTree(ingested_at)",
		"ORDER BY (company_id, interview_id, sequence_number)",
		"application_id Nullable(String)",
	} {
		if !strings.Contains(SchemaDDL, fragment) {
			t.Errorf("schema missing %q", fragment)
		}
	}
}
