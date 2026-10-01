package livemonitor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/sse"
)

var (
	// ErrInterviewNotFound means the interview is not in the viewer's
	// company (or has been purged).
	ErrInterviewNotFound = errors.New("interview not found")
	// ErrMonitoringDisabled means the company turned live monitoring off.
	ErrMonitoringDisabled = errors.New("live monitoring is disabled")
)

// Access decides whether a company may watch an interview live.
type Access interface {
	LiveMonitoring(ctx context.Context, companyID, interviewID uuid.UUID) error
}

// History is the durable event log the stream falls back to when a viewer
// is further behind than the ring buffer reaches (replay.Store).
type History interface {
	Timeline(ctx context.Context, companyID, interviewID uuid.UUID, afterSeq int64) ([]Event, error)
}

// Config tunes the stream. Zero values select defaults.
type Config struct {
	// Heartbeat is how often an idle stream writes a comment line.
	Heartbeat time.Duration
	Logger    *slog.Logger
}

// Handler serves GET /interviews/{id}/live.
type Handler struct {
	bus     Bus
	history History
	access  Access
	cfg     Config
}

// NewHandler streams from bus, falling back to history (may be nil) and
// checking access (may be nil, which allows every interview).
func NewHandler(bus Bus, history History, access Access, cfg Config) *Handler {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Handler{bus: bus, history: history, access: access, cfg: cfg}
}

// Routes returns the live endpoint behind member authentication.
func (h *Handler) Routes(resolver rbac.PrincipalResolver) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /interviews/{id}/live", rbac.Require(rbac.PermInterviewsLive, h.Live))
	return rbac.Middleware(resolver, mux)
}

// Live streams the interview's events. It must run behind rbac.Middleware.
//
// Events carry their sequence number as the SSE id, so a reconnecting
// EventSource resumes from Last-Event-ID. The subscription is opened before
// catching up so nothing published during catch-up is lost; duplicates are
// dropped by sequence number.
func (h *Handler) Live(w http.ResponseWriter, r *http.Request) {
	principal, ok := rbac.FromContext(r.Context())
	if !ok {
		rbac.WriteError(w, http.StatusUnauthorized, "unauthorized", "authenticated company member is required")
		return
	}
	interviewID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		rbac.WriteError(w, http.StatusBadRequest, "invalid_id", "id must be a UUID")
		return
	}
	after, err := LastEventID(r)
	if err != nil {
		rbac.WriteError(w, http.StatusBadRequest, "invalid_last_event_id", "Last-Event-ID must be a non-negative integer")
		return
	}
	if h.access != nil {
		switch err := h.access.LiveMonitoring(r.Context(), principal.CompanyID, interviewID); {
		case errors.Is(err, ErrInterviewNotFound):
			rbac.WriteError(w, http.StatusNotFound, "not_found", "not found")
			return
		case errors.Is(err, ErrMonitoringDisabled):
			rbac.WriteError(w, http.StatusForbidden, "live_monitoring_disabled", "live monitoring is disabled for this company")
			return
		case err != nil:
			rbac.WriteError(w, http.StatusServiceUnavailable, "access_unavailable", "could not check access")
			return
		}
	}

	ctx := r.Context()
	stream := Stream{CompanyID: principal.CompanyID, InterviewID: interviewID}
	sub, err := h.bus.Subscribe(ctx, stream)
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "live_unavailable", "live stream is temporarily unavailable")
		return
	}
	defer func() { _ = sub.Close() }()

	writer, err := sse.NewWriter(w)
	if err != nil {
		rbac.WriteError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming is not supported")
		return
	}
	s := &session{h: h, ctx: ctx, stream: stream, writer: writer, lastSent: after}
	if err := s.catchUp(true); err != nil {
		return
	}
	// caught_up has no id, so it does not move the client's Last-Event-ID.
	if err := writer.Send("caught_up", map[string]int64{"last_event_id": s.lastSent}); err != nil {
		return
	}

	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if writer.Comment("ping") != nil {
				return
			}
		case event, ok := <-sub.Events():
			if !ok {
				// The subscription ended; the client reconnects with
				// Last-Event-ID and loses nothing.
				return
			}
			if event.SequenceNumber > s.lastSent+1 {
				// Possibly dropped events: repair from the ring.
				if s.catchUp(false) != nil {
					return
				}
			}
			if s.send(event) != nil {
				return
			}
		}
	}
}

type session struct {
	h        *Handler
	ctx      context.Context
	stream   Stream
	writer   *sse.Writer
	lastSent int64
}

// catchUp sends every event after lastSent from the ring, first reading the
// durable history for any part the ring no longer holds. deep allows that
// history read; gap repair during live streaming only uses the ring.
func (s *session) catchUp(deep bool) error {
	recent, oldest, err := s.h.bus.Recent(s.ctx, s.stream, s.lastSent)
	if err != nil {
		s.h.cfg.Logger.Error("read live ring", slog.Any("error", err))
		return err
	}
	if deep && s.h.history != nil && (oldest == 0 || oldest > s.lastSent+1) {
		past, err := s.h.history.Timeline(s.ctx, s.stream.CompanyID, s.stream.InterviewID, s.lastSent)
		if err != nil {
			// Degrade to what the ring has rather than failing the stream.
			s.h.cfg.Logger.Warn("read live history", slog.Any("error", err))
		}
		for _, event := range past {
			if oldest != 0 && event.SequenceNumber >= oldest {
				break
			}
			if err := s.send(event); err != nil {
				return err
			}
		}
	}
	for _, event := range recent {
		if err := s.send(event); err != nil {
			return err
		}
	}
	return nil
}

// send writes event unless it was already sent or belongs to another
// company (which the Bus keys make impossible; this is defense in depth).
func (s *session) send(event Event) error {
	if event.SequenceNumber <= s.lastSent || event.CompanyID != s.stream.CompanyID.String() ||
		event.InterviewID != s.stream.InterviewID.String() {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := s.writer.SendWithID(strconv.FormatInt(event.SequenceNumber, 10), "event", data); err != nil {
		return err
	}
	s.lastSent = event.SequenceNumber
	return nil
}

// LastEventID reads the resume point from the Last-Event-ID header, or the
// last_event_id query parameter for clients that cannot set headers. Absent
// means 0: stream from the start.
func LastEventID(r *http.Request) (int64, error) {
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("last_event_id")
	}
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("invalid Last-Event-ID")
	}
	return value, nil
}
