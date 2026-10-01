package postgres

import (
	"regexp"
	"strings"
	"testing"
)

// TestAdminQueriesAreCompanyScoped checks every Admin API statement filters
// or writes by the tenant. companies is keyed by its own id, which is the
// company_id.
func TestAdminQueriesAreCompanyScoped(t *testing.T) {
	scoped := regexp.MustCompile(`company_id = \$1\b`)
	for name, query := range adminQueries {
		switch {
		case strings.Contains(query, "FROM companies") || strings.HasPrefix(query, "UPDATE companies"):
			if !regexp.MustCompile(`WHERE id = \$1\b`).MatchString(query) {
				t.Errorf("%s is not scoped to one company: %s", name, query)
			}
		case strings.HasPrefix(query, "INSERT INTO"):
			if !strings.Contains(query, "(company_id,") || !strings.Contains(query, "VALUES ($1,") {
				t.Errorf("%s does not write company_id from $1: %s", name, query)
			}
		default:
			if !scoped.MatchString(query) {
				t.Errorf("%s is not company_id-scoped: %s", name, query)
			}
		}
	}
}

func TestEraseQueryDeletesNothing(t *testing.T) {
	query := adminQueries["requestErasure"]
	if !strings.HasPrefix(query, "UPDATE interviews SET erase_requested_at = COALESCE(erase_requested_at, $3)") {
		t.Fatalf("erasure is not a marking update: %s", query)
	}
	for name, query := range adminQueries {
		if strings.Contains(query, "DELETE FROM interviews") {
			t.Errorf("%s deletes interviews", name)
		}
	}
}

func TestDecideScoreQueryOnlyAcceptsHumanStatuses(t *testing.T) {
	if !strings.Contains(adminQueries["decideScore"], `$4::text LIKE 'human\_%'`) {
		t.Fatal("decideScore does not guard for human_* statuses")
	}
}
