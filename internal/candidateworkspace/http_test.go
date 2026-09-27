package candidateworkspace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
)

func candidateSession(state domain.SessionState) domain.Session {
	interviewID := uuid.New()
	return domain.Session{InterviewID: &interviewID, State: state, ExpiresAt: time.Now().Add(time.Hour)}
}

type fixedFinder struct {
	session domain.Session
	err     error
}

func (f fixedFinder) FindSessionByTokenHash(context.Context, []byte) (domain.Session, error) {
	return f.session, f.err
}

func TestRequireActiveSessionRejectsNonActiveWithConflict(t *testing.T) {
	states := []domain.SessionState{
		domain.SessionStateCompleted, domain.SessionStateExpired, domain.SessionStateRevoked,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			finder := fixedFinder{session: candidateSession(state)}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/candidate/me", nil)
			request.Header.Set("Authorization", "Bearer opaque-token")
			RequireActiveSession(finder, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("non-Active request reached handler")
			})).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusConflict {
				t.Errorf("status = %d, want 409", recorder.Code)
			}
		})
	}
}

func TestRequireActiveSessionAddsScopeToContext(t *testing.T) {
	want := candidateSession(domain.SessionStateActive)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/candidate/me", nil)
	request.Header.Set("Authorization", "Bearer opaque-token")
	RequireActiveSession(fixedFinder{session: want}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := ActiveSessionFromContext(r.Context())
		if !ok || got.State != want.State {
			t.Errorf("session context = %+v, %v", got, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", recorder.Code)
	}
}

func TestRequireActiveSessionRejectsCompanyMemberTokens(t *testing.T) {
	userID := uuid.New()
	member := domain.Session{UserID: &userID, State: domain.SessionStateActive, ExpiresAt: time.Now().Add(time.Hour)}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/candidate/me", nil)
	request.Header.Set("Authorization", "Bearer opaque-token")
	RequireActiveSession(fixedFinder{session: member}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("company member token reached the candidate workspace")
	})).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", recorder.Code)
	}
}
