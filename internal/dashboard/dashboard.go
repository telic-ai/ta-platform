package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/replay"
)

// DailyCount is one day's count of one event type.
type DailyCount struct {
	Day       string `json:"day"`
	EventType string `json:"event_type"`
	Events    uint64 `json:"events"`
}

// ActivityRow is one (interview, event type, detail) aggregate.
type ActivityRow struct {
	InterviewID  string
	EventType    string
	Detail       string
	Events       uint64
	LinesAdded   int64
	LinesRemoved int64
	FirstAt      time.Time
	LastAt       time.Time
}

// Store reads the dashboard views, always for one company.
type Store interface {
	DailyEvents(ctx context.Context, companyID uuid.UUID, since time.Time) ([]DailyCount, error)
	ActivityRows(ctx context.Context, companyID uuid.UUID) ([]ActivityRow, error)
}

// ReplayAccess decides whether a company may replay an interview. It
// returns adminapi.ErrNotFound for an interview outside the company and
// ErrReplayDisabled when the company turned replay off.
type ReplayAccess interface {
	ReplayAccess(ctx context.Context, companyID, interviewID uuid.UUID) error
}

// ErrReplayDisabled means the company's replay policy is off.
var ErrReplayDisabled = errors.New("replay is disabled")

// InterviewActivity summarizes one interview's candidate activity.
type InterviewActivity struct {
	InterviewID    string    `json:"interview_id"`
	Events         uint64    `json:"events"`
	Prompts        uint64    `json:"prompts"`
	AIResponses    uint64    `json:"ai_responses"`
	Runs           uint64    `json:"runs"`
	RunsSucceeded  uint64    `json:"runs_succeeded"`
	Diffs          uint64    `json:"diffs"`
	AIAppliedDiffs uint64    `json:"ai_applied_diffs"`
	LinesAdded     int64     `json:"lines_added"`
	LinesRemoved   int64     `json:"lines_removed"`
	FirstAt        time.Time `json:"first_at"`
	LastAt         time.Time `json:"last_at"`
}

// Summarize folds activity rows into one summary per interview, most
// recently active first.
func Summarize(rows []ActivityRow) []InterviewActivity {
	byID := map[string]*InterviewActivity{}
	for _, r := range rows {
		a, ok := byID[r.InterviewID]
		if !ok {
			a = &InterviewActivity{InterviewID: r.InterviewID, FirstAt: r.FirstAt, LastAt: r.LastAt}
			byID[r.InterviewID] = a
		}
		a.Events += r.Events
		if r.FirstAt.Before(a.FirstAt) {
			a.FirstAt = r.FirstAt
		}
		if r.LastAt.After(a.LastAt) {
			a.LastAt = r.LastAt
		}
		switch events.EventType(r.EventType) {
		case events.EventTypePromptSubmitted:
			a.Prompts += r.Events
		case events.EventTypeAIResponseCompleted:
			a.AIResponses += r.Events
		case events.EventTypeExecutionRequested:
			a.Runs += r.Events
		case events.EventTypeExecutionCompleted:
			if r.Detail == events.ExecutionStatusSucceeded {
				a.RunsSucceeded += r.Events
			}
		case events.EventTypeCodeDiff:
			a.Diffs += r.Events
			a.LinesAdded += r.LinesAdded
			a.LinesRemoved += r.LinesRemoved
			if r.Detail == events.DiffOriginAIApplied {
				a.AIAppliedDiffs += r.Events
			}
		}
	}
	out := make([]InterviewActivity, 0, len(byID))
	for _, a := range byID {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastAt.Equal(out[j].LastAt) {
			return out[i].LastAt.After(out[j].LastAt)
		}
		return out[i].InterviewID < out[j].InterviewID
	})
	return out
}

// Handler serves the dashboard and replay routes.
type Handler struct {
	store    Store
	timeline replay.Store
	access   ReplayAccess
	now      func() time.Time
}

func NewHandler(store Store, timeline replay.Store, access ReplayAccess) *Handler {
	return &Handler{store: store, timeline: timeline, access: access, now: time.Now}
}

// Mount adds the routes to the Admin API, behind its authentication.
func (h *Handler) Mount(api *adminapi.Handler) {
	api.Handle("GET /dashboard/overview", rbac.PermAnalyticsRead, h.Overview)
	api.Handle("GET /dashboard/interviews", rbac.PermAnalyticsRead, h.Interviews)
	api.Handle("GET /interviews/{id}/timeline", rbac.PermInterviewsRead, h.Timeline)
}

const (
	defaultDays = 14
	maxDays     = 90
)

// Overview returns daily event counts by type for the last days days
// (default 14, at most 90), including today.
func (h *Handler) Overview(w http.ResponseWriter, r *http.Request) {
	p, _ := rbac.FromContext(r.Context())
	days := defaultDays
	if raw := r.URL.Query().Get("days"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxDays {
			rbac.WriteError(w, http.StatusBadRequest, "invalid_days", "days must be between 1 and 90")
			return
		}
		days = n
	}
	today := h.now().UTC().Truncate(24 * time.Hour)
	since := today.AddDate(0, 0, -(days - 1))
	counts, err := h.store.DailyEvents(r.Context(), p.CompanyID, since)
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "dashboard_unavailable", "dashboard is temporarily unavailable")
		return
	}
	totals := map[string]uint64{}
	for _, c := range counts {
		totals[c.EventType] += c.Events
	}
	if counts == nil {
		counts = []DailyCount{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"since": since.Format(time.DateOnly), "days": days, "daily": counts, "totals": totals,
	})
}

// Interviews returns per-interview activity summaries.
func (h *Handler) Interviews(w http.ResponseWriter, r *http.Request) {
	p, _ := rbac.FromContext(r.Context())
	rows, err := h.store.ActivityRows(r.Context(), p.CompanyID)
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "dashboard_unavailable", "dashboard is temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"interviews": Summarize(rows)})
}

// Timeline returns an interview's ordered events after after_seq, for
// replay.
func (h *Handler) Timeline(w http.ResponseWriter, r *http.Request) {
	p, _ := rbac.FromContext(r.Context())
	interviewID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		rbac.WriteError(w, http.StatusBadRequest, "invalid_id", "id must be a UUID")
		return
	}
	afterSeq := int64(0)
	if raw := r.URL.Query().Get("after_seq"); raw != "" {
		afterSeq, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || afterSeq < 0 {
			rbac.WriteError(w, http.StatusBadRequest, "invalid_after_seq", "after_seq must be a non-negative integer")
			return
		}
	}
	switch err := h.access.ReplayAccess(r.Context(), p.CompanyID, interviewID); {
	case errors.Is(err, adminapi.ErrNotFound):
		rbac.WriteError(w, http.StatusNotFound, "not_found", "not found")
		return
	case errors.Is(err, ErrReplayDisabled):
		rbac.WriteError(w, http.StatusForbidden, "replay_disabled", "replay is disabled for this company")
		return
	case err != nil:
		rbac.WriteError(w, http.StatusServiceUnavailable, "access_unavailable", "could not check access")
		return
	}
	timeline, err := h.timeline.Timeline(r.Context(), p.CompanyID, interviewID, afterSeq)
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "timeline_unavailable", "timeline is temporarily unavailable")
		return
	}
	if timeline == nil {
		timeline = []replay.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": timeline})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
