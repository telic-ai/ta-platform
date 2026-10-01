package housekeeper

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/eventindexer"
)

var now = time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)

func TestRunDeletesSearchThenAnalyticsThenCommitsPostgres(t *testing.T) {
	var calls []string
	c := Candidate{CompanyID: uuid.New(), InterviewID: uuid.New(), Reason: ReasonRetention}
	store := &fakeStore{candidates: []Candidate{c}, eligible: map[uuid.UUID]bool{c.InterviewID: true}, calls: &calls}
	h := newTestHousekeeper(t, store, &fakeSearch{calls: &calls, deleted: 3}, &fakeAnalytics{calls: &calls})
	report, err := h.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lock", "search", "analytics", "commit"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if len(report.Purged) != 1 || store.stats[0].SearchDocumentsDeleted != 3 {
		t.Fatalf("report = %+v stats = %+v", report, store.stats)
	}
	if wantCutoff := now.Add(-30 * 24 * time.Hour); !store.cutoff.Equal(wantCutoff) {
		t.Fatalf("cutoff = %v, want %v", store.cutoff, wantCutoff)
	}
}

func TestSearchFailureStopsBeforeClickHouseAndPostgres(t *testing.T) {
	var calls []string
	c := Candidate{CompanyID: uuid.New(), InterviewID: uuid.New()}
	store := &fakeStore{candidates: []Candidate{c}, eligible: map[uuid.UUID]bool{c.InterviewID: true}, calls: &calls}
	h := newTestHousekeeper(t, store, &fakeSearch{calls: &calls, err: errors.New("typesense down")}, &fakeAnalytics{calls: &calls})
	report, err := h.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lock", "search", "rollback"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if len(report.Failures) != 1 || len(report.Purged) != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestAnalyticsFailureRollsBackPostgres(t *testing.T) {
	var calls []string
	c := Candidate{CompanyID: uuid.New(), InterviewID: uuid.New()}
	store := &fakeStore{candidates: []Candidate{c}, eligible: map[uuid.UUID]bool{c.InterviewID: true}, calls: &calls}
	h := newTestHousekeeper(t, store, &fakeSearch{calls: &calls}, &fakeAnalytics{calls: &calls, err: errors.New("clickhouse down")})
	report, _ := h.Run(context.Background())
	if want := []string{"lock", "search", "analytics", "rollback"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if len(report.Failures) != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestOneFailureDoesNotStopTheBatch(t *testing.T) {
	var calls []string
	bad, good := Candidate{CompanyID: uuid.New(), InterviewID: uuid.New()}, Candidate{CompanyID: uuid.New(), InterviewID: uuid.New()}
	store := &fakeStore{candidates: []Candidate{bad, good}, eligible: map[uuid.UUID]bool{bad.InterviewID: true, good.InterviewID: true}, calls: &calls}
	search := &fakeSearch{calls: &calls, failFor: bad.InterviewID}
	report, _ := newTestHousekeeper(t, store, search, &fakeAnalytics{calls: &calls}).Run(context.Background())
	if len(report.Failures) != 1 || report.Failures[0].Candidate != bad || len(report.Purged) != 1 || report.Purged[0] != good {
		t.Fatalf("report = %+v", report)
	}
}

func TestNoLongerEligibleIsSkippedWithoutExternalDeletes(t *testing.T) {
	var calls []string
	c := Candidate{CompanyID: uuid.New(), InterviewID: uuid.New()}
	store := &fakeStore{candidates: []Candidate{c}, eligible: map[uuid.UUID]bool{}, calls: &calls}
	report, _ := newTestHousekeeper(t, store, &fakeSearch{calls: &calls}, &fakeAnalytics{calls: &calls}).Run(context.Background())
	if want := []string{"lock"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if len(report.Skipped) != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestListFailureIsAnError(t *testing.T) {
	store := &fakeStore{listErr: errors.New("db down")}
	if _, err := newTestHousekeeper(t, store, &fakeSearch{}, &fakeAnalytics{}).Run(context.Background()); err == nil {
		t.Fatal("list failure swallowed")
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(&fakeStore{}, &fakeSearch{}, &fakeAnalytics{}, Config{Retention: 0, BatchSize: 1}); err == nil {
		t.Error("zero retention accepted")
	}
	if _, err := New(nil, &fakeSearch{}, &fakeAnalytics{}, Config{Retention: time.Hour, BatchSize: 1}); err == nil {
		t.Error("nil store accepted")
	}
}

func TestTypesenseSearchFiltersByTenantAndInterview(t *testing.T) {
	companyID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	interviewID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	deleter := &recordingDeleter{}
	n, err := TypesenseSearch{Client: deleter}.DeleteInterview(context.Background(), companyID, interviewID)
	if err != nil || n != 2 {
		t.Fatalf("DeleteInterview = %d, %v", n, err)
	}
	want := "company_id:=`11111111-1111-1111-1111-111111111111` && interview_id:=`22222222-2222-2222-2222-222222222222`"
	if deleter.collection != eventindexer.Collection || deleter.filter != want {
		t.Fatalf("delete %s %q", deleter.collection, deleter.filter)
	}
}

func TestClickHouseTablesAreThePerInterviewTables(t *testing.T) {
	if !reflect.DeepEqual(ClickHouseTables, []string{"events", "session_metric_rows"}) {
		t.Fatalf("ClickHouseTables = %v", ClickHouseTables)
	}
}

func newTestHousekeeper(t *testing.T, store Store, search Search, analytics Analytics) *Housekeeper {
	t.Helper()
	h, err := New(store, search, analytics, Config{Retention: 30 * 24 * time.Hour, BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	h.now = func() time.Time { return now }
	return h
}

type fakeStore struct {
	candidates []Candidate
	eligible   map[uuid.UUID]bool
	listErr    error
	calls      *[]string
	cutoff     time.Time
	stats      []Stats
}

func (s *fakeStore) record(call string) {
	if s.calls != nil {
		*s.calls = append(*s.calls, call)
	}
}

func (s *fakeStore) Candidates(_ context.Context, cutoff time.Time, _ int) ([]Candidate, error) {
	s.cutoff = cutoff
	return s.candidates, s.listErr
}

func (s *fakeStore) Purge(ctx context.Context, c Candidate, _ time.Time, deleteExternal func(context.Context) (Stats, error)) (bool, error) {
	s.record("lock")
	if !s.eligible[c.InterviewID] {
		return false, nil
	}
	stats, err := deleteExternal(ctx)
	if err != nil {
		s.record("rollback")
		return false, err
	}
	s.record("commit")
	s.stats = append(s.stats, stats)
	return true, nil
}

type fakeSearch struct {
	calls   *[]string
	deleted int
	err     error
	failFor uuid.UUID
}

func (s *fakeSearch) DeleteInterview(_ context.Context, _, interviewID uuid.UUID) (int, error) {
	if s.calls != nil {
		*s.calls = append(*s.calls, "search")
	}
	if s.err != nil || interviewID == s.failFor {
		return 0, errors.Join(s.err, errors.New("search failed"))
	}
	return s.deleted, nil
}

type fakeAnalytics struct {
	calls *[]string
	err   error
}

func (a *fakeAnalytics) DeleteInterview(context.Context, uuid.UUID, uuid.UUID) error {
	if a.calls != nil {
		*a.calls = append(*a.calls, "analytics")
	}
	return a.err
}

type recordingDeleter struct{ collection, filter string }

func (d *recordingDeleter) DeleteByFilter(_ context.Context, collection, filter string) (int, error) {
	d.collection, d.filter = collection, filter
	return 2, nil
}
