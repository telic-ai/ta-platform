package housekeeper

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/eventindexer"
)

// SearchDeleter is the Typesense client method the Housekeeper uses.
type SearchDeleter interface {
	DeleteByFilter(ctx context.Context, collection, filter string) (int, error)
}

// TypesenseSearch deletes an interview's documents from the Event Indexer's
// collection.
type TypesenseSearch struct{ Client SearchDeleter }

func (s TypesenseSearch) DeleteInterview(ctx context.Context, companyID, interviewID uuid.UUID) (int, error) {
	return s.Client.DeleteByFilter(ctx, eventindexer.Collection, InterviewFilter(companyID, interviewID))
}

// InterviewFilter is the Typesense filter for one interview's documents.
// Backticks make each value an exact string match; UUIDs cannot contain
// one, so no escaping is needed.
func InterviewFilter(companyID, interviewID uuid.UUID) string {
	return fmt.Sprintf("company_id:=`%s` && interview_id:=`%s`", companyID, interviewID)
}

// ClickHouseTables are the tables holding per-interview rows: the event log
// and the per-session metric projection. Aggregate views (task daily,
// funnel, billing usage) keep only ids and counts and are left intact, so
// past usage and invoices stay reproducible.
var ClickHouseTables = []string{"events", "session_metric_rows"}

// ClickHouseAnalytics deletes an interview's rows with lightweight deletes,
// which hide the rows immediately and remove them at the next merge.
type ClickHouseAnalytics struct{ Conn driver.Conn }

func (a ClickHouseAnalytics) DeleteInterview(ctx context.Context, companyID, interviewID uuid.UUID) error {
	for _, table := range ClickHouseTables {
		statement := "DELETE FROM " + table + " WHERE company_id = ? AND interview_id = ?"
		if err := a.Conn.Exec(ctx, statement, companyID.String(), interviewID.String()); err != nil {
			return fmt.Errorf("delete from %s: %w", table, err)
		}
	}
	return nil
}
