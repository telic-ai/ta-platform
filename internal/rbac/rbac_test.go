package rbac

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
)

func TestRoleValid(t *testing.T) {
	for _, role := range Roles {
		if !role.Valid() {
			t.Errorf("%q not valid", role)
		}
	}
	for _, role := range []Role{"", "root", "Owner", "candidate"} {
		if role.Valid() {
			t.Errorf("%q valid", role)
		}
	}
}

func TestCanMatrix(t *testing.T) {
	cases := []struct {
		permission Permission
		allowed    []Role
	}{
		{PermCompanyRead, []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer, RoleViewer}},
		{PermCompanyWrite, []Role{RoleOwner, RoleAdmin}},
		{PermUsersWrite, []Role{RoleOwner, RoleAdmin}},
		{PermInterviewsWrite, []Role{RoleOwner, RoleAdmin, RoleRecruiter}},
		{PermInterviewsErase, []Role{RoleOwner, RoleAdmin}},
		{PermInterviewsLive, []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer}},
		{PermTasksWrite, []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer}},
		{PermInvitesCandidate, []Role{RoleOwner, RoleAdmin, RoleRecruiter}},
		{PermInvitesMember, []Role{RoleOwner, RoleAdmin}},
		{PermScoresRead, []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer}},
		{PermScoresApprove, []Role{RoleOwner, RoleAdmin, RoleInterviewer}},
		{PermPoliciesWrite, []Role{RoleOwner, RoleAdmin}},
		{PermSearch, []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer, RoleViewer}},
	}
	for _, tc := range cases {
		allowed := map[Role]bool{}
		for _, role := range tc.allowed {
			allowed[role] = true
		}
		for _, role := range Roles {
			if got := Can(role, tc.permission); got != allowed[role] {
				t.Errorf("Can(%s, %s) = %v, want %v", role, tc.permission, got, allowed[role])
			}
		}
	}
}

func TestCanDeniesUnknownPermissionAndRole(t *testing.T) {
	if Can(RoleOwner, Permission("nuke")) {
		t.Error("unknown permission granted")
	}
	if Can(Role("root"), PermCompanyRead) {
		t.Error("unknown role granted")
	}
}

func TestEveryPermissionIsInThePolicy(t *testing.T) {
	for _, permission := range []Permission{
		PermCompanyRead, PermCompanyWrite, PermUsersRead, PermUsersWrite,
		PermInterviewsRead, PermInterviewsWrite, PermInterviewsErase, PermInterviewsLive,
		PermTasksRead, PermTasksWrite, PermInvitesCandidate, PermInvitesMember,
		PermScoresRead, PermScoresApprove, PermPoliciesRead, PermPoliciesWrite,
		PermAnalyticsRead, PermSearch,
	} {
		if len(policy[permission]) == 0 || !Can(RoleOwner, permission) {
			t.Errorf("%s missing from policy or denied to owner", permission)
		}
	}
}

type sessions struct {
	session domain.Session
	err     error
	hash    []byte
}

func (s *sessions) FindSessionByTokenHash(_ context.Context, hash []byte) (domain.Session, error) {
	s.hash = hash
	return s.session, s.err
}

type roles struct {
	role              Role
	err               error
	companyID, userID uuid.UUID
}

func (r *roles) UserRole(_ context.Context, companyID, userID uuid.UUID) (Role, error) {
	r.companyID, r.userID = companyID, userID
	return r.role, r.err
}

func request(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestResolverResolvesMember(t *testing.T) {
	now := time.Now()
	companyID, userID := uuid.New(), uuid.New()
	s := &sessions{session: domain.Session{CompanyID: companyID, UserID: &userID, State: domain.SessionStateActive, ExpiresAt: now.Add(time.Hour)}}
	r := &roles{role: RoleRecruiter}
	resolver := Resolver{Sessions: s, Roles: r, Now: func() time.Time { return now }}

	got, err := resolver.Resolve(request("tok"))
	if err != nil {
		t.Fatal(err)
	}
	want := Principal{CompanyID: companyID, UserID: userID, Role: RoleRecruiter}
	if got != want {
		t.Fatalf("principal = %+v, want %+v", got, want)
	}
	if string(s.hash) != string(auth.HashToken("tok")) {
		t.Error("session looked up by something other than the token hash")
	}
	if r.companyID != companyID || r.userID != userID {
		t.Error("role looked up outside the session's company")
	}
}

func TestResolverRejects(t *testing.T) {
	now := time.Now()
	userID, interviewID := uuid.New(), uuid.New()
	active := domain.Session{CompanyID: uuid.New(), UserID: &userID, State: domain.SessionStateActive, ExpiresAt: now.Add(time.Hour)}
	candidate := active
	candidate.UserID, candidate.InterviewID = nil, &interviewID
	expired := active
	expired.ExpiresAt = now.Add(-time.Second)
	revoked := active
	revoked.State = domain.SessionStateRevoked
	cases := map[string]struct {
		token    string
		sessions *sessions
		roles    *roles
	}{
		"no token":        {"", &sessions{session: active}, &roles{role: RoleOwner}},
		"unknown session": {"t", &sessions{err: auth.ErrSessionNotFound}, &roles{role: RoleOwner}},
		"candidate":       {"t", &sessions{session: candidate}, &roles{role: RoleOwner}},
		"expired":         {"t", &sessions{session: expired}, &roles{role: RoleOwner}},
		"revoked":         {"t", &sessions{session: revoked}, &roles{role: RoleOwner}},
		"deleted user":    {"t", &sessions{session: active}, &roles{err: ErrUserNotFound}},
		"unknown role":    {"t", &sessions{session: active}, &roles{role: "root"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Resolver{Sessions: tc.sessions, Roles: tc.roles, Now: func() time.Time { return now }}.Resolve(request(tc.token))
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestResolverSurfacesBackendErrors(t *testing.T) {
	userID := uuid.New()
	active := domain.Session{CompanyID: uuid.New(), UserID: &userID, State: domain.SessionStateActive, ExpiresAt: time.Now().Add(time.Hour)}
	boom := errors.New("db down")
	for name, resolver := range map[string]Resolver{
		"sessions": {Sessions: &sessions{err: boom}, Roles: &roles{}},
		"roles":    {Sessions: &sessions{session: active}, Roles: &roles{err: boom}},
	} {
		if _, err := resolver.Resolve(request("t")); !errors.Is(err, boom) || errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestMiddlewareStoresPrincipal(t *testing.T) {
	want := Principal{CompanyID: uuid.New(), UserID: uuid.New(), Role: RoleAdmin}
	var got Principal
	handler := Middleware(ResolverFunc(func(*http.Request) (Principal, error) { return want, nil }),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got, _ = FromContext(r.Context()) }))
	handler.ServeHTTP(httptest.NewRecorder(), request("t"))
	if got != want {
		t.Fatalf("principal = %+v", got)
	}
}

func TestMiddlewareErrors(t *testing.T) {
	for err, status := range map[error]int{
		ErrUnauthenticated: http.StatusUnauthorized,
		errors.New("boom"): http.StatusServiceUnavailable,
	} {
		called := false
		handler := Middleware(ResolverFunc(func(*http.Request) (Principal, error) { return Principal{}, err }),
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request("t"))
		if response.Code != status || called {
			t.Errorf("%v: status = %d, called = %v", err, response.Code, called)
		}
	}
}

func TestRequire(t *testing.T) {
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	guarded := Require(PermPoliciesWrite, ok)

	response := httptest.NewRecorder()
	guarded(response, request(""))
	if response.Code != http.StatusUnauthorized {
		t.Errorf("no principal status = %d", response.Code)
	}

	for role, status := range map[Role]int{RoleAdmin: http.StatusNoContent, RoleViewer: http.StatusForbidden} {
		r := request("")
		r = r.WithContext(WithPrincipal(r.Context(), Principal{Role: role}))
		response := httptest.NewRecorder()
		guarded(response, r)
		if response.Code != status {
			t.Errorf("%s status = %d, want %d", role, response.Code, status)
		}
		if status == http.StatusForbidden {
			var body map[string]string
			_ = json.NewDecoder(response.Body).Decode(&body)
			if body["code"] != "forbidden" {
				t.Errorf("body = %v", body)
			}
		}
	}
}
