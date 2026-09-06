package migrations

import "testing"

func TestEmbeddedMigrationsAreCompleteAndOrdered(t *testing.T) {
	got, err := load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one embedded migration")
	}
	for i, migration := range got {
		if migration.up == "" || migration.down == "" {
			t.Fatalf("migration %d is incomplete", migration.version)
		}
		if i > 0 && got[i-1].version >= migration.version {
			t.Fatalf("migrations are not strictly ordered: %d then %d", got[i-1].version, migration.version)
		}
	}
}
