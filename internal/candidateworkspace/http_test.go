package candidateworkspace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/domain"
)

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
			finder := fixedFinder{session: domain.Session{State: state, ExpiresAt: time.Now().Add(time.Hour)}}
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
	want := domain.Session{State: domain.SessionStateActive, ExpiresAt: time.Now().Add(time.Hour)}
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
