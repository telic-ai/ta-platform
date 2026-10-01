package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/replay"
)

type fakeStore struct {
	daily    []DailyCount
	activity []ActivityRow
	err      error
	company  uuid.UUID
	since    time.Time
}

func (s *fakeStore) DailyEvents(_ context.Context, companyID uuid.UUID, since time.Time) ([]DailyCount, error) {
	s.company, s.since = companyID, since
	return s.daily, s.err
}

func (s *fakeStore) ActivityRows(_ context.Context, companyID uuid.UUID) ([]ActivityRow, error) {
	s.company = companyID
	return s.activity, s.err
}

type fakeTimeline struct {
	events  []replay.Event
	err     error
	company uuid.UUID
	after   int64
}

func (f *fakeTimeline) Timeline(_ context.Context, companyID, _ uuid.UUID, after int64) ([]replay.Event, error) {
	f.company, f.after = companyID, after
	return f.events, f.err
}

type accessFunc func(uuid.UUID, uuid.UUID) error

func (f accessFunc) ReplayAccess(_ context.Context, c, i uuid.UUID) error { return f(c, i) }

var allow = accessFunc(func(uuid.UUID, uuid.UUID) error { return nil })

type memoryAdminStore struct{ adminapi.Store }

func serve(t *testing.T, h *Handler, role rbac.Role, companyID uuid.UUID, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	p := rbac.Principal{CompanyID: companyID, UserID: uuid.New(), Role: role}
	api := adminapi.NewHandler(memoryAdminStore{}, rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) { return p, nil }), adminapi.Config{})
	h.Mount(api)
	w := httptest.NewRecorder()
	api.Routes().ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestSummarize(t *testing.T) {
	a, b := uuid.NewString(), uuid.NewString()
	t0 := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	rows := []ActivityRow{
		{InterviewID: a, EventType: "prompt.submitted", Events: 3, FirstAt: t0.Add(time.Minute), LastAt: t0.Add(5 * time.Minute)},
		{InterviewID: a, EventType: "ai.response.completed", Detail: "completed", Events: 3, FirstAt: t0.Add(2 * time.Minute), LastAt: t0.Add(6 * time.Minute)},
		{InterviewID: a, EventType: "execution.requested", Events: 4, FirstAt: t0, LastAt: t0.Add(7 * time.Minute)},
		{InterviewID: a, EventType: "execution.completed", Detail: "succeeded", Events: 1, FirstAt: t0, LastAt: t0},
		{InterviewID: a, EventType: "execution.completed", Detail: "failed", Events: 3, FirstAt: t0, LastAt: t0},
		{InterviewID: a, EventType: "code.diff", Detail: "manual", Events: 10, LinesAdded: 40, LinesRemoved: 5, FirstAt: t0, LastAt: t0},
		{InterviewID: a, EventType: "code.diff", Detail: "ai_applied", Events: 2, LinesAdded: 20, LinesRemoved: 1, FirstAt: t0, LastAt: t0},
		{InterviewID: b, EventType: "session.started", Events: 1, FirstAt: t0.Add(time.Hour), LastAt: t0.Add(time.Hour)},
	}
	got := Summarize(rows)
	if len(got) != 2 || got[0].InterviewID != b || got[1].InterviewID != a {
		t.Fatalf("order = %+v", got)
	}
	s := got[1]
	want := InterviewActivity{InterviewID: a, Events: 26, Prompts: 3, AIResponses: 3, Runs: 4, RunsSucceeded: 1, Diffs: 12,
		AIAppliedDiffs: 2, LinesAdded: 60, LinesRemoved: 6, FirstAt: t0, LastAt: t0.Add(7 * time.Minute)}
	if s != want {
		t.Fatalf("summary =\n%+v\nwant\n%+v", s, want)
	}
	if len(Summarize(nil)) != 0 {
		t.Fatal("empty summary")
	}
}

func TestOverview(t *testing.T) {
	companyID := uuid.New()
	store := &fakeStore{daily: []DailyCount{
		{Day: "2026-09-26", EventType: "code.diff", Events: 5},
		{Day: "2026-09-27", EventType: "code.diff", Events: 2},
		{Day: "2026-09-27", EventType: "session.started", Events: 1},
	}}
	h := NewHandler(store, &fakeTimeline{}, allow)
	h.now = func() time.Time { return time.Date(2026, 9, 27, 15, 0, 0, 0, time.UTC) }

	w := serve(t, h, rbac.RoleViewer, companyID, "GET", "/dashboard/overview?days=7")
	if w.Code != 200 {
		t.Fatalf("status = %d %s", w.Code, w.Body)
	}
	var body struct {
		Since  string            `json:"since"`
		Days   int               `json:"days"`
		Daily  []DailyCount      `json:"daily"`
		Totals map[string]uint64 `json:"totals"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body.Since != "2026-09-21" || body.Days != 7 || len(body.Daily) != 3 || body.Totals["code.diff"] != 7 || body.Totals["session.started"] != 1 {
		t.Fatalf("body = %+v", body)
	}
	if store.company != companyID || !store.since.Equal(time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("store called with %s since %s", store.company, store.since)
	}

	for _, bad := range []string{"0", "91", "x"} {
		if w := serve(t, h, rbac.RoleViewer, companyID, "GET", "/dashboard/overview?days="+bad); w.Code != 400 {
			t.Errorf("days=%s status = %d", bad, w.Code)
		}
	}
	store.daily = nil
	w = serve(t, h, rbac.RoleViewer, companyID, "GET", "/dashboard/overview")
	if !strings.Contains(w.Body.String(), `"daily":[]`) || !strings.Contains(w.Body.String(), `"days":14`) {
		t.Fatalf("empty body = %s", w.Body)
	}
	store.err = errors.New("clickhouse down")
	if w := serve(t, h, rbac.RoleViewer, companyID, "GET", "/dashboard/overview"); w.Code != 503 {
		t.Fatalf("failure status = %d", w.Code)
	}
}

func TestInterviewsActivity(t *testing.T) {
	companyID, interviewID := uuid.New(), uuid.NewString()
	store := &fakeStore{activity: []ActivityRow{{InterviewID: interviewID, EventType: "prompt.submitted", Events: 2}}}
	w := serve(t, NewHandler(store, &fakeTimeline{}, allow), rbac.RoleRecruiter, companyID, "GET", "/dashboard/interviews")
	var body struct {
		Interviews []InterviewActivity `json:"interviews"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != 200 || len(body.Interviews) != 1 || body.Interviews[0].Prompts != 2 || store.company != companyID {
		t.Fatalf("status %d body %s", w.Code, w.Body)
	}
	store.err = errors.New("down")
	if w := serve(t, NewHandler(store, &fakeTimeline{}, allow), rbac.RoleRecruiter, companyID, "GET", "/dashboard/interviews"); w.Code != 503 {
		t.Fatalf("failure status = %d", w.Code)
	}
}

func TestTimeline(t *testing.T) {
	companyID, interviewID := uuid.New(), uuid.New()
	timeline := &fakeTimeline{events: []replay.Event{{EventID: "e", SequenceNumber: 3}}}
	var checked [2]uuid.UUID
	access := accessFunc(func(c, i uuid.UUID) error { checked = [2]uuid.UUID{c, i}; return nil })
	path := "/interviews/" + interviewID.String() + "/timeline"
	w := serve(t, NewHandler(&fakeStore{}, timeline, access), rbac.RoleViewer, companyID, "GET", path+"?after_seq=2")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"sequence_number":3`) {
		t.Fatalf("status %d body %s", w.Code, w.Body)
	}
	if timeline.company != companyID || timeline.after != 2 || checked != [2]uuid.UUID{companyID, interviewID} {
		t.Fatalf("timeline for %s after %d, access %v", timeline.company, timeline.after, checked)
	}

	timeline.events = nil
	w = serve(t, NewHandler(&fakeStore{}, timeline, allow), rbac.RoleViewer, companyID, "GET", path)
	if w.Body.String() != "{\"events\":[]}\n" {
		t.Fatalf("empty body = %q", w.Body)
	}

	cases := map[string]struct {
		access accessFunc
		path   string
		err    error
		status int
	}{
		"bad id":          {allow, "/interviews/nope/timeline", nil, 400},
		"bad cursor":      {allow, path + "?after_seq=-1", nil, 400},
		"not found":       {func(uuid.UUID, uuid.UUID) error { return adminapi.ErrNotFound }, path, nil, 404},
		"disabled":        {func(uuid.UUID, uuid.UUID) error { return ErrReplayDisabled }, path, nil, 403},
		"access failure":  {func(uuid.UUID, uuid.UUID) error { return errors.New("db") }, path, nil, 503},
		"history failure": {allow, path, errors.New("clickhouse"), 503},
	}
	for name, tc := range cases {
		timeline := &fakeTimeline{err: tc.err}
		if w := serve(t, NewHandler(&fakeStore{}, timeline, tc.access), rbac.RoleViewer, companyID, "GET", tc.path); w.Code != tc.status {
			t.Errorf("%s: status %d, want %d", name, w.Code, tc.status)
		}
	}
}

func TestDashboardQueriesAreCompanyScoped(t *testing.T) {
	for name, q := range map[string]string{"daily": dailyQuery, "activity": activityQuery} {
		if !strings.Contains(q, "WHERE company_id = ?") {
			t.Errorf("%s query is not company-scoped", name)
		}
	}
}
