package replay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type memoryStore struct {
	events    []Event
	companyID uuid.UUID
	afterSeq  int64
}

func (s *memoryStore) Timeline(_ context.Context, companyID, interviewID uuid.UUID, afterSeq int64) ([]Event, error) {
	s.companyID = companyID
	s.afterSeq = afterSeq
	result := make([]Event, 0)
	for _, event := range s.events {
		if event.CompanyID == companyID.String() && event.InterviewID == interviewID.String() && event.SequenceNumber > afterSeq {
			result = append(result, event)
		}
	}
	return result, nil
}

type fixedTenant uuid.UUID

func (f fixedTenant) CompanyID(*http.Request) (uuid.UUID, error) { return uuid.UUID(f), nil }

func TestTimelineReturnsOrderedEventsAfterCursor(t *testing.T) {
	companyID := uuid.New()
	interviewID := uuid.New()
	store := &memoryStore{events: []Event{
		{EventID: "event-2", CompanyID: companyID.String(), InterviewID: interviewID.String(), SequenceNumber: 2, EventType: "answer.submitted", OccurredAt: time.Now(), Payload: json.RawMessage(`{"answer":"yes"}`)},
		{EventID: "event-3", CompanyID: companyID.String(), InterviewID: interviewID.String(), SequenceNumber: 3, EventType: "session.completed", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)},
	}}
	handler := NewHTTPHandler(store, fixedTenant(companyID)).Routes()
	request := httptest.NewRequest(http.MethodGet, "/interviews/"+interviewID.String()+"/timeline?after_seq=1", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body struct {
		Events []Event `json:"events"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Events) != 2 || body.Events[0].SequenceNumber != 2 || body.Events[1].SequenceNumber != 3 {
		t.Fatalf("events = %#v", body.Events)
	}
	if store.afterSeq != 1 {
		t.Fatalf("afterSeq = %d", store.afterSeq)
	}
}

func TestTimelineBlocksCrossTenantRead(t *testing.T) {
	owner := uuid.New()
	otherTenant := uuid.New()
	interviewID := uuid.New()
	store := &memoryStore{events: []Event{{
		EventID: "private", CompanyID: owner.String(), InterviewID: interviewID.String(), SequenceNumber: 1,
		EventType: "answer.submitted", OccurredAt: time.Now(), Payload: json.RawMessage(`{"private":true}`),
	}}}
	handler := NewHTTPHandler(store, fixedTenant(otherTenant)).Routes()
	request := httptest.NewRequest(http.MethodGet, "/interviews/"+interviewID.String()+"/timeline", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || response.Body.String() != "{\"events\":[]}\n" {
		t.Fatalf("cross-tenant response = %d %q", response.Code, response.Body.String())
	}
	if store.companyID != otherTenant {
		t.Fatalf("store company ID = %s, want authenticated tenant %s", store.companyID, otherTenant)
	}
}

func TestTimelineValidatesIdentityAndCursor(t *testing.T) {
	store := &memoryStore{}
	unauthenticated := NewHTTPHandler(store, HeaderTenantResolver{}).Routes()
	request := httptest.NewRequest(http.MethodGet, "/interviews/"+uuid.NewString()+"/timeline", nil)
	response := httptest.NewRecorder()
	unauthenticated.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing identity status = %d", response.Code)
	}

	handler := NewHTTPHandler(store, fixedTenant(uuid.New())).Routes()
	request = httptest.NewRequest(http.MethodGet, "/interviews/"+uuid.NewString()+"/timeline?after_seq=-1", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d", response.Code)
	}
}

func TestTimelineQueryScopesAndOrdersInClickHouse(t *testing.T) {
	for _, required := range []string{
		"company_id = ?",
		"interview_id = ?",
		"sequence_number > ?",
		"ORDER BY sequence_number ASC",
	} {
		if !strings.Contains(timelineQuery, required) {
			t.Errorf("timeline query missing %q", required)
		}
	}
}
