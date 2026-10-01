package adminapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

var now = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

// fixture is two companies: A with one member per role, B with an owner and
// an interview of its own.
type fixture struct {
	store      *memoryStore
	handler    http.Handler
	companyA   uuid.UUID
	companyB   uuid.UUID
	members    map[rbac.Role]uuid.UUID
	ownerB     uuid.UUID
	interviewA uuid.UUID
	interviewB uuid.UUID
	scoreA     uuid.UUID
	token      string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{store: newMemoryStore(), companyA: uuid.New(), companyB: uuid.New(), members: map[rbac.Role]uuid.UUID{},
		ownerB: uuid.New(), interviewA: uuid.New(), interviewB: uuid.New(), scoreA: uuid.New(), token: "fixed-token"}
	f.store.companies[f.companyA] = domain.Company{ID: f.companyA, Name: "A", Slug: "a"}
	f.store.companies[f.companyB] = domain.Company{ID: f.companyB, Name: "B", Slug: "b"}
	for _, role := range rbac.Roles {
		id := uuid.New()
		f.members[role] = id
		f.store.users[key{f.companyA, id}] = domain.User{ID: id, CompanyID: f.companyA, Email: string(role) + "@a.test", Role: string(role)}
	}
	f.store.users[key{f.companyB, f.ownerB}] = domain.User{ID: f.ownerB, CompanyID: f.companyB, Email: "owner@b.test", Role: "owner"}
	f.store.interviews[key{f.companyA, f.interviewA}] = domain.Interview{ID: f.interviewA, CompanyID: f.companyA, CandidateName: "Ada", CandidateEmail: "ada@x.test", Status: "in_progress"}
	f.store.interviews[key{f.companyB, f.interviewB}] = domain.Interview{ID: f.interviewB, CompanyID: f.companyB, CandidateName: "Bob", CandidateEmail: "bob@x.test", Status: "in_progress"}
	f.store.scores[key{f.companyA, f.scoreA}] = domain.Score{ID: f.scoreA, CompanyID: f.companyA, InterviewID: f.interviewA, Dimension: "correctness", ProposedValue: 3.5, Status: domain.ScoreStatusProposed}

	// Tokens are "<company>:<user>"; the resolver checks the role in the
	// store like rbac.Resolver does.
	resolver := rbac.ResolverFunc(func(r *http.Request) (rbac.Principal, error) {
		token, ok := auth.BearerToken(r)
		if !ok {
			return rbac.Principal{}, rbac.ErrUnauthenticated
		}
		companyRaw, userRaw, _ := strings.Cut(token, ":")
		companyID, err1 := uuid.Parse(companyRaw)
		userID, err2 := uuid.Parse(userRaw)
		if err1 != nil || err2 != nil {
			return rbac.Principal{}, rbac.ErrUnauthenticated
		}
		role, err := f.store.UserRole(r.Context(), companyID, userID)
		if err != nil {
			return rbac.Principal{}, rbac.ErrUnauthenticated
		}
		return rbac.Principal{CompanyID: companyID, UserID: userID, Role: role}, nil
	})
	ids := 0
	f.handler = NewHandler(f.store, resolver, Config{
		Now:      func() time.Time { return now },
		NewID:    func() uuid.UUID { ids++; return uuid.NewSHA1(uuid.Nil, []byte{byte(ids), byte(ids >> 8)}) },
		NewToken: func() (string, error) { return f.token, nil },
	}).Routes()
	return f
}

func (f *fixture) as(role rbac.Role) string {
	return f.companyA.String() + ":" + f.members[role].String()
}
func (f *fixture) asOwnerB() string { return f.companyB.String() + ":" + f.ownerB.String() }

func (f *fixture) do(t *testing.T, token, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	switch b := body.(type) {
	case nil:
		reader = bytes.NewReader(nil)
	case string:
		reader = bytes.NewReader([]byte(b))
	default:
		raw, _ := json.Marshal(b)
		reader = bytes.NewReader(raw)
	}
	r := httptest.NewRequest(method, path, reader)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}

func decodeBody[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return v
}

func expect(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, status, w.Body.String())
	}
}

type route struct {
	method, path string
	permission   rbac.Permission
	body         any
}

func (f *fixture) routes() []route {
	iv := "/interviews/" + f.interviewA.String()
	someID := uuid.NewString()
	return []route{
		{"GET", "/company", rbac.PermCompanyRead, nil},
		{"PATCH", "/company", rbac.PermCompanyWrite, map[string]any{}},
		{"GET", "/users", rbac.PermUsersRead, nil},
		{"POST", "/users", rbac.PermUsersWrite, map[string]any{"email": "n@a.test", "role": "viewer"}},
		{"GET", "/users/" + someID, rbac.PermUsersRead, nil},
		{"PATCH", "/users/" + someID, rbac.PermUsersWrite, map[string]any{}},
		{"DELETE", "/users/" + someID, rbac.PermUsersWrite, nil},
		{"GET", "/interviews", rbac.PermInterviewsRead, nil},
		{"POST", "/interviews", rbac.PermInterviewsWrite, map[string]any{"candidate_name": "C", "candidate_email": "c@x.test"}},
		{"GET", iv, rbac.PermInterviewsRead, nil},
		{"PATCH", iv, rbac.PermInterviewsWrite, map[string]any{}},
		{"POST", iv + "/erase", rbac.PermInterviewsErase, nil},
		{"GET", iv + "/scores", rbac.PermScoresRead, nil},
		{"POST", iv + "/scores/" + f.scoreA.String() + "/decision", rbac.PermScoresApprove, map[string]any{"status": "human_approved"}},
		{"GET", "/tasks", rbac.PermTasksRead, nil},
		{"POST", "/tasks", rbac.PermTasksWrite, map[string]any{"title": "t"}},
		{"GET", "/tasks/" + someID, rbac.PermTasksRead, nil},
		{"PATCH", "/tasks/" + someID, rbac.PermTasksWrite, map[string]any{}},
		{"DELETE", "/tasks/" + someID, rbac.PermTasksWrite, nil},
		{"POST", "/invites", rbac.PermInvitesCandidate, map[string]any{"email": "c@x.test", "interview_id": f.interviewA}},
		{"GET", "/policies", rbac.PermPoliciesRead, nil},
		{"PUT", "/policies/replay", rbac.PermPoliciesWrite, map[string]any{"enabled": false}},
	}
}

func TestEveryRouteRequiresAuthentication(t *testing.T) {
	f := newFixture(t)
	for _, rt := range f.routes() {
		for _, token := range []string{"", "garbage", uuid.NewString() + ":" + uuid.NewString()} {
			if w := f.do(t, token, rt.method, rt.path, rt.body); w.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with token %q: status %d", rt.method, rt.path, token, w.Code)
			}
		}
	}
}

func TestRoleChecksEnforcedOnEveryRoute(t *testing.T) {
	for _, role := range rbac.Roles {
		f := newFixture(t)
		for _, rt := range f.routes() {
			w := f.do(t, f.as(role), rt.method, rt.path, rt.body)
			forbidden := w.Code == http.StatusForbidden
			if forbidden == rbac.Can(role, rt.permission) {
				t.Errorf("%s %s as %s: status %d, allowed by matrix = %v", rt.method, rt.path, role, w.Code, rbac.Can(role, rt.permission))
			}
		}
	}
}

func TestEveryStoreCallIsScopedToThePrincipalsCompany(t *testing.T) {
	f := newFixture(t)
	for _, rt := range f.routes() {
		f.do(t, f.as(rbac.RoleOwner), rt.method, rt.path, rt.body)
	}
	if len(f.store.companiesSeen) == 0 {
		t.Fatal("no store calls recorded")
	}
	for _, companyID := range f.store.companiesSeen {
		if companyID != f.companyA {
			t.Fatalf("store called with company %s, want %s", companyID, f.companyA)
		}
	}
}

func TestCompanyGetAndUpdate(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, f.as(rbac.RoleViewer), "GET", "/company", nil)
	expect(t, w, 200)
	if got := decodeBody[companyView](t, w); got.ID != f.companyA || got.Name != "A" {
		t.Fatalf("company = %+v", got)
	}
	w = f.do(t, f.as(rbac.RoleAdmin), "PATCH", "/company", map[string]any{"name": "  Acme  "})
	expect(t, w, 200)
	if got := decodeBody[companyView](t, w); got.Name != "Acme" {
		t.Fatalf("name = %q", got.Name)
	}
	expect(t, f.do(t, f.as(rbac.RoleAdmin), "PATCH", "/company", map[string]any{"name": " "}), 400)
	if f.store.companies[f.companyB].Name != "B" {
		t.Fatal("other company changed")
	}
}

func TestUserCRUD(t *testing.T) {
	f := newFixture(t)
	admin := f.as(rbac.RoleAdmin)

	w := f.do(t, admin, "POST", "/users", map[string]any{"email": "New@A.test", "display_name": "N", "role": "interviewer"})
	expect(t, w, 201)
	created := decodeBody[userView](t, w)
	if created.Email != "new@a.test" || created.Role != "interviewer" {
		t.Fatalf("created = %+v", created)
	}
	expect(t, f.do(t, admin, "POST", "/users", map[string]any{"email": "new@a.test", "role": "viewer"}), 409)
	expect(t, f.do(t, admin, "POST", "/users", map[string]any{"email": "x@a.test", "role": "root"}), 400)
	expect(t, f.do(t, admin, "POST", "/users", map[string]any{"email": "not an email", "role": "viewer"}), 400)

	w = f.do(t, f.as(rbac.RoleViewer), "GET", "/users", nil)
	expect(t, w, 200)
	if list := decodeBody[map[string][]userView](t, w)["users"]; len(list) != len(rbac.Roles)+1 {
		t.Fatalf("listed %d users", len(list))
	}

	path := "/users/" + created.ID.String()
	expect(t, f.do(t, f.as(rbac.RoleViewer), "GET", path, nil), 200)
	w = f.do(t, admin, "PATCH", path, map[string]any{"role": "recruiter", "display_name": "Newt"})
	expect(t, w, 200)
	if got := decodeBody[userView](t, w); got.Role != "recruiter" || got.DisplayName != "Newt" {
		t.Fatalf("updated = %+v", got)
	}
	expect(t, f.do(t, admin, "DELETE", path, nil), 204)
	expect(t, f.do(t, admin, "GET", path, nil), 404)
	expect(t, f.do(t, admin, "DELETE", path, nil), 404)
}

func TestOnlyOwnersManageOwners(t *testing.T) {
	f := newFixture(t)
	admin, owner := f.as(rbac.RoleAdmin), f.as(rbac.RoleOwner)
	ownerPath := "/users/" + f.members[rbac.RoleOwner].String()
	viewerPath := "/users/" + f.members[rbac.RoleViewer].String()

	expect(t, f.do(t, admin, "POST", "/users", map[string]any{"email": "o2@a.test", "role": "owner"}), 403)
	expect(t, f.do(t, admin, "PATCH", viewerPath, map[string]any{"role": "owner"}), 403)
	expect(t, f.do(t, admin, "PATCH", ownerPath, map[string]any{"display_name": "x"}), 403)
	expect(t, f.do(t, admin, "DELETE", ownerPath, nil), 403)
	expect(t, f.do(t, owner, "PATCH", viewerPath, map[string]any{"role": "owner"}), 200)
}

func TestMembersCannotDemoteOrRemoveThemselves(t *testing.T) {
	f := newFixture(t)
	admin := f.as(rbac.RoleAdmin)
	self := "/users/" + f.members[rbac.RoleAdmin].String()
	expect(t, f.do(t, admin, "PATCH", self, map[string]any{"role": "viewer"}), 409)
	expect(t, f.do(t, admin, "DELETE", self, nil), 409)
	expect(t, f.do(t, admin, "PATCH", self, map[string]any{"display_name": "Me", "role": "admin"}), 200)
}

func TestCrossTenantRecordsAreNotFound(t *testing.T) {
	f := newFixture(t)
	owner := f.as(rbac.RoleOwner)
	otherInterview := "/interviews/" + f.interviewB.String()
	for _, rt := range []route{
		{"GET", "/users/" + f.ownerB.String(), "", nil},
		{"PATCH", "/users/" + f.ownerB.String(), "", map[string]any{}},
		{"DELETE", "/users/" + f.ownerB.String(), "", nil},
		{"GET", otherInterview, "", nil},
		{"PATCH", otherInterview, "", map[string]any{"status": "cancelled"}},
		{"POST", otherInterview + "/erase", "", nil},
		{"GET", otherInterview + "/scores", "", nil},
	} {
		if w := f.do(t, owner, rt.method, rt.path, rt.body); w.Code != 404 {
			t.Errorf("%s %s: status %d", rt.method, rt.path, w.Code)
		}
	}
	if f.store.interviews[key{f.companyB, f.interviewB}].EraseRequestedAt != nil ||
		f.store.interviews[key{f.companyB, f.interviewB}].Status != "in_progress" {
		t.Fatal("other company's interview changed")
	}
	// Company B sees only its own interview.
	w := f.do(t, f.asOwnerB(), "GET", "/interviews", nil)
	expect(t, w, 200)
	list := decodeBody[map[string][]interviewView](t, w)["interviews"]
	if len(list) != 1 || list[0].ID != f.interviewB {
		t.Fatalf("company B listed %+v", list)
	}
}

func TestInterviewCRUD(t *testing.T) {
	f := newFixture(t)
	recruiter := f.as(rbac.RoleRecruiter)
	scheduled := now.Add(24 * time.Hour)
	w := f.do(t, recruiter, "POST", "/interviews", map[string]any{"candidate_name": "Cy", "candidate_email": "cy@x.test", "scheduled_at": scheduled})
	expect(t, w, 201)
	created := decodeBody[interviewView](t, w)
	if created.Status != "scheduled" || created.CreatedBy == nil || *created.CreatedBy != f.members[rbac.RoleRecruiter] || !created.ScheduledAt.Equal(scheduled) {
		t.Fatalf("created = %+v", created)
	}
	expect(t, f.do(t, recruiter, "POST", "/interviews", map[string]any{"candidate_name": "", "candidate_email": "cy@x.test"}), 400)

	path := "/interviews/" + created.ID.String()
	w = f.do(t, recruiter, "PATCH", path, map[string]any{"status": "in_progress"})
	expect(t, w, 200)
	if got := decodeBody[interviewView](t, w); got.Status != "in_progress" || got.TerminalAt != nil {
		t.Fatalf("in progress = %+v", got)
	}
	w = f.do(t, recruiter, "PATCH", path, map[string]any{"status": "completed"})
	expect(t, w, 200)
	if got := decodeBody[interviewView](t, w); got.TerminalAt == nil || !got.TerminalAt.Equal(now) {
		t.Fatalf("completed = %+v", got)
	}
	expect(t, f.do(t, recruiter, "PATCH", path, map[string]any{"status": "exploded"}), 400)

	w = f.do(t, f.as(rbac.RoleViewer), "GET", "/interviews?status=completed", nil)
	expect(t, w, 200)
	if list := decodeBody[map[string][]interviewView](t, w)["interviews"]; len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("filtered = %+v", list)
	}
	expect(t, f.do(t, recruiter, "GET", "/interviews?status=bogus", nil), 400)
	expect(t, f.do(t, recruiter, "GET", "/interviews?limit=0", nil), 400)
	expect(t, f.do(t, recruiter, "GET", "/interviews?limit=1", nil), 200)
	expect(t, f.do(t, recruiter, "GET", "/interviews/not-a-uuid", nil), 400)
}

func TestInterviewsHaveNoDeleteRoute(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, f.as(rbac.RoleOwner), "DELETE", "/interviews/"+f.interviewA.String(), nil)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE interview status = %d", w.Code)
	}
	if _, ok := f.store.interviews[key{f.companyA, f.interviewA}]; !ok {
		t.Fatal("interview deleted")
	}
}

func TestEraseMarksAndDeletesNothing(t *testing.T) {
	f := newFixture(t)
	taskID := uuid.New()
	f.store.tasks[key{f.companyA, taskID}] = domain.Task{ID: taskID, CompanyID: f.companyA, InterviewID: &f.interviewA, Title: "t", Status: "open"}
	before := f.store.interviews[key{f.companyA, f.interviewA}]

	w := f.do(t, f.as(rbac.RoleAdmin), "POST", "/interviews/"+f.interviewA.String()+"/erase", nil)
	expect(t, w, http.StatusAccepted)
	body := decodeBody[map[string]any](t, w)
	if body["id"] != f.interviewA.String() || body["erase_requested_at"] != now.Format(time.RFC3339) {
		t.Fatalf("body = %v", body)
	}

	after, ok := f.store.interviews[key{f.companyA, f.interviewA}]
	if !ok {
		t.Fatal("interview deleted")
	}
	if after.EraseRequestedAt == nil || !after.EraseRequestedAt.Equal(now) {
		t.Fatalf("erase_requested_at = %v", after.EraseRequestedAt)
	}
	after.EraseRequestedAt = nil
	if after != before {
		t.Fatalf("erase changed more than erase_requested_at:\nbefore %+v\nafter  %+v", before, after)
	}
	if _, ok := f.store.tasks[key{f.companyA, taskID}]; !ok || len(f.store.scores) != 1 {
		t.Fatal("erase deleted related records")
	}

	// A repeat request keeps the first request's time.
	later := f.store.interviews[key{f.companyA, f.interviewA}]
	f.do(t, f.as(rbac.RoleOwner), "POST", "/interviews/"+f.interviewA.String()+"/erase", nil)
	if got := f.store.interviews[key{f.companyA, f.interviewA}]; !got.EraseRequestedAt.Equal(*later.EraseRequestedAt) {
		t.Fatal("repeat erase moved erase_requested_at")
	}

	for _, role := range []rbac.Role{rbac.RoleRecruiter, rbac.RoleInterviewer, rbac.RoleViewer} {
		expect(t, f.do(t, f.as(role), "POST", "/interviews/"+f.interviewA.String()+"/erase", nil), 403)
	}
	expect(t, f.do(t, f.as(rbac.RoleOwner), "POST", "/interviews/"+uuid.NewString()+"/erase", nil), 404)
}

func TestScoreDecisions(t *testing.T) {
	f := newFixture(t)
	interviewer := f.as(rbac.RoleInterviewer)
	path := "/interviews/" + f.interviewA.String() + "/scores/" + f.scoreA.String() + "/decision"

	w := f.do(t, f.as(rbac.RoleRecruiter), "GET", "/interviews/"+f.interviewA.String()+"/scores", nil)
	expect(t, w, 200)
	if list := decodeBody[map[string][]scoreView](t, w)["scores"]; len(list) != 1 || list[0].Status != "proposed" {
		t.Fatalf("scores = %+v", list)
	}
	expect(t, f.do(t, f.as(rbac.RoleViewer), "GET", "/interviews/"+f.interviewA.String()+"/scores", nil), 403)

	for _, status := range []string{"proposed", "ai_approved", "approved", "", "HUMAN_APPROVED"} {
		expect(t, f.do(t, interviewer, "POST", path, map[string]any{"status": status}), 400)
	}
	expect(t, f.do(t, interviewer, "POST", path, map[string]any{"status": "human_adjusted"}), 400)
	expect(t, f.do(t, interviewer, "POST", path, map[string]any{"status": "human_approved", "final_value": 1}), 400)
	if f.store.scores[key{f.companyA, f.scoreA}].Status != "proposed" {
		t.Fatal("rejected decision changed the score")
	}

	w = f.do(t, interviewer, "POST", path, map[string]any{"status": "human_approved", "note": "agree"})
	expect(t, w, 200)
	got := decodeBody[scoreView](t, w)
	if got.Status != "human_approved" || got.FinalValue == nil || *got.FinalValue != 3.5 ||
		got.DecidedBy == nil || *got.DecidedBy != f.members[rbac.RoleInterviewer] || !got.DecidedAt.Equal(now) {
		t.Fatalf("approved = %+v", got)
	}
	w = f.do(t, f.as(rbac.RoleAdmin), "POST", path, map[string]any{"status": "human_adjusted", "final_value": 2.0})
	expect(t, w, 200)
	if got := decodeBody[scoreView](t, w); got.FinalValue == nil || *got.FinalValue != 2.0 {
		t.Fatalf("adjusted = %+v", got)
	}
	expect(t, f.do(t, f.as(rbac.RoleRecruiter), "POST", path, map[string]any{"status": "human_approved"}), 403)
	expect(t, f.do(t, interviewer, "POST", "/interviews/"+uuid.NewString()+"/scores/"+f.scoreA.String()+"/decision",
		map[string]any{"status": "human_approved"}), 404)
}

func TestTaskCRUD(t *testing.T) {
	f := newFixture(t)
	interviewer := f.as(rbac.RoleInterviewer)
	assignee := f.members[rbac.RoleInterviewer]
	w := f.do(t, interviewer, "POST", "/tasks", map[string]any{"title": "Review", "interview_id": f.interviewA, "assignee_id": assignee})
	expect(t, w, 201)
	task := decodeBody[taskView](t, w)
	if task.Status != "open" || task.AssigneeID == nil || *task.AssigneeID != assignee {
		t.Fatalf("task = %+v", task)
	}
	// References must be in the caller's company.
	expect(t, f.do(t, interviewer, "POST", "/tasks", map[string]any{"title": "x", "interview_id": f.interviewB}), 422)
	expect(t, f.do(t, interviewer, "POST", "/tasks", map[string]any{"title": "x", "assignee_id": f.ownerB}), 422)
	expect(t, f.do(t, interviewer, "POST", "/tasks", map[string]any{"title": ""}), 400)

	path := "/tasks/" + task.ID.String()
	w = f.do(t, interviewer, "PATCH", path, map[string]any{"status": "done", "assignee_id": nil})
	expect(t, w, 200)
	updated := decodeBody[taskView](t, w)
	if updated.Status != "done" || updated.AssigneeID != nil || updated.CompletedAt == nil || !updated.CompletedAt.Equal(now) {
		t.Fatalf("updated = %+v", updated)
	}
	// An absent assignee_id leaves the assignee alone.
	f.do(t, interviewer, "PATCH", path, map[string]any{"assignee_id": assignee})
	w = f.do(t, interviewer, "PATCH", path, map[string]any{"title": "Renamed"})
	if got := decodeBody[taskView](t, w); got.AssigneeID == nil || got.Title != "Renamed" {
		t.Fatalf("partial update = %+v", got)
	}
	expect(t, f.do(t, interviewer, "PATCH", path, map[string]any{"status": "lost"}), 400)

	w = f.do(t, f.as(rbac.RoleViewer), "GET", "/tasks?interview_id="+f.interviewA.String(), nil)
	expect(t, w, 200)
	if list := decodeBody[map[string][]taskView](t, w)["tasks"]; len(list) != 1 {
		t.Fatalf("tasks = %+v", list)
	}
	expect(t, f.do(t, interviewer, "GET", "/tasks?assignee_id=nope", nil), 400)
	expect(t, f.do(t, interviewer, "GET", "/tasks?status=lost", nil), 400)
	expect(t, f.do(t, interviewer, "GET", path, nil), 200)
	expect(t, f.do(t, interviewer, "DELETE", path, nil), 204)
	expect(t, f.do(t, interviewer, "GET", path, nil), 404)
}

func TestCandidateInvite(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, f.as(rbac.RoleRecruiter), "POST", "/invites", map[string]any{"email": "ada@x.test", "interview_id": f.interviewA})
	expect(t, w, 201)
	got := decodeBody[inviteView](t, w)
	if got.Token != f.token || got.Role != "candidate" || got.InterviewID == nil || *got.InterviewID != f.interviewA ||
		!got.ExpiresAt.Equal(now.Add(7*24*time.Hour)) {
		t.Fatalf("invite = %+v", got)
	}
	stored := f.store.invites[0]
	if !bytes.Equal(stored.TokenHash, auth.HashToken(f.token)) || stored.CompanyID != f.companyA ||
		*stored.InvitedBy != f.members[rbac.RoleRecruiter] {
		t.Fatalf("stored invite = %+v", stored)
	}
	if strings.Contains(string(stored.TokenHash), f.token) {
		t.Fatal("raw token stored")
	}
	// Another company's interview cannot be targeted.
	expect(t, f.do(t, f.as(rbac.RoleRecruiter), "POST", "/invites", map[string]any{"email": "b@x.test", "interview_id": f.interviewB}), 422)
	expect(t, f.do(t, f.as(rbac.RoleRecruiter), "POST", "/invites", map[string]any{"email": "b@x.test", "interview_id": f.interviewA, "role": "admin"}), 400)
	expect(t, f.do(t, f.as(rbac.RoleInterviewer), "POST", "/invites", map[string]any{"email": "b@x.test", "interview_id": f.interviewA}), 403)
}

func TestMemberInvite(t *testing.T) {
	f := newFixture(t)
	expect(t, f.do(t, f.as(rbac.RoleRecruiter), "POST", "/invites", map[string]any{"email": "m@a.test", "role": "viewer"}), 403)
	expect(t, f.do(t, f.as(rbac.RoleAdmin), "POST", "/invites", map[string]any{"email": "m@a.test", "role": "owner"}), 403)
	expect(t, f.do(t, f.as(rbac.RoleAdmin), "POST", "/invites", map[string]any{"email": "m@a.test", "role": "nobody"}), 400)
	w := f.do(t, f.as(rbac.RoleAdmin), "POST", "/invites", map[string]any{"email": "m@a.test", "role": "interviewer", "expires_in_seconds": 3600})
	expect(t, w, 201)
	if got := decodeBody[inviteView](t, w); got.Role != "interviewer" || got.InterviewID != nil || !got.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("invite = %+v", got)
	}
	for _, expires := range []int64{-1, int64(31 * 24 * time.Hour / time.Second)} {
		expect(t, f.do(t, f.as(rbac.RoleAdmin), "POST", "/invites", map[string]any{"email": "m@a.test", "role": "viewer", "expires_in_seconds": expires}), 400)
	}
}

func TestInviteTokenFailureIsUnavailable(t *testing.T) {
	store := newMemoryStore()
	p := rbac.Principal{CompanyID: uuid.New(), UserID: uuid.New(), Role: rbac.RoleOwner}
	h := NewHandler(store, rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) { return p, nil }),
		Config{NewToken: func() (string, error) { return "", errors.New("no entropy") }}).Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/invites", strings.NewReader(`{"email":"a@b.test","role":"viewer"}`)))
	expect(t, w, 503)
	if len(store.invites) != 0 {
		t.Fatal("invite stored without a token")
	}
}

func TestPolicies(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, f.as(rbac.RoleViewer), "GET", "/policies", nil)
	expect(t, w, 200)
	list := decodeBody[map[string][]policyView](t, w)["policies"]
	if len(list) != len(PolicyKeys()) {
		t.Fatalf("policies = %+v", list)
	}
	for _, p := range list {
		if !p.Enabled || p.UpdatedBy != nil {
			t.Fatalf("default policy = %+v", p)
		}
	}

	w = f.do(t, f.as(rbac.RoleAdmin), "PUT", "/policies/live_monitoring", map[string]any{"enabled": false})
	expect(t, w, 200)
	if got := decodeBody[policyView](t, w); got.Enabled || *got.UpdatedBy != f.members[rbac.RoleAdmin] {
		t.Fatalf("set = %+v", got)
	}
	w = f.do(t, f.as(rbac.RoleViewer), "GET", "/policies", nil)
	for _, p := range decodeBody[map[string][]policyView](t, w)["policies"] {
		if p.Enabled == (p.Key == "live_monitoring") {
			t.Fatalf("policy %s enabled = %v", p.Key, p.Enabled)
		}
	}
	// Company B still sees defaults.
	w = f.do(t, f.asOwnerB(), "GET", "/policies", nil)
	for _, p := range decodeBody[map[string][]policyView](t, w)["policies"] {
		if !p.Enabled {
			t.Fatalf("company B policy %s disabled", p.Key)
		}
	}
	expect(t, f.do(t, f.as(rbac.RoleAdmin), "PUT", "/policies/self_destruct", map[string]any{"enabled": true}), 404)
	expect(t, f.do(t, f.as(rbac.RoleAdmin), "PUT", "/policies/replay", map[string]any{}), 400)
}

func TestRequestBodyValidation(t *testing.T) {
	f := newFixture(t)
	admin := f.as(rbac.RoleAdmin)
	expect(t, f.do(t, admin, "PATCH", "/company", `{"name":"x","slug":"stolen"}`), 400)
	expect(t, f.do(t, admin, "PATCH", "/company", `{"name":"x"} {"name":"y"}`), 400)
	expect(t, f.do(t, admin, "PATCH", "/company", `not json`), 400)
	expect(t, f.do(t, admin, "PATCH", "/company", `{"name":"`+strings.Repeat("x", maxBodyBytes)+`"}`), 413)
	// A body cannot choose the tenant.
	w := f.do(t, admin, "POST", "/interviews", map[string]any{"candidate_name": "C", "candidate_email": "c@x.test", "company_id": f.companyB})
	expect(t, w, 400)
}

func TestStoreFailuresAreUnavailable(t *testing.T) {
	f := newFixture(t)
	f.store.err = errors.New("db down")
	expect(t, f.do(t, f.as(rbac.RoleOwner), "GET", "/company", nil), 503)
	expect(t, f.do(t, f.as(rbac.RoleOwner), "POST", "/interviews/"+f.interviewA.String()+"/erase", nil), 503)
}

func TestNewInviteToken(t *testing.T) {
	a, err := NewInviteToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewInviteToken()
	if a == b || len(a) < 40 || strings.ContainsAny(a, "+/=") {
		t.Fatalf("tokens %q %q", a, b)
	}
}
