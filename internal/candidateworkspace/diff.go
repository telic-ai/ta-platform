package candidateworkspace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/sandbox"
)

const maxPatchBytes = 256 << 10

var (
	// ErrInvalidDiff is returned for diffs rejected before any side effect.
	ErrInvalidDiff = errors.New("invalid diff")
	// ErrDiffOutOfOrder means a diff with the same or a later client
	// sequence was already accepted for the session.
	ErrDiffOutOfOrder = errors.New("diff is out of order")
)

// DiffStore accepts diffs in client-sequence order.
type DiffStore interface {
	// RecordDiff raises the session's last accepted client sequence to
	// clientSequence, allocates one interview sequence number and writes
	// the message build returns, all in one transaction. It returns
	// ErrDiffOutOfOrder, with no side effects, if clientSequence is not
	// greater than the last accepted one.
	RecordDiff(ctx context.Context, companyID, sessionID, interviewID uuid.UUID, clientSequence int64,
		build func(sequence int64) (outbox.Message, error)) (int64, error)
}

// DiffRequest is the body of POST /session/diff. The client debounces edits
// into one diff per burst and numbers them from 1 per session.
type DiffRequest struct {
	ClientSequence int64  `json:"clientSeq"`
	Origin         string `json:"origin"`
	PromptID       string `json:"promptId,omitempty"`
	Path           string `json:"path"`
	Patch          string `json:"patch"`
}

// DiffResult tells the client whether the diff was kept.
type DiffResult struct {
	Accepted       bool   `json:"accepted"`
	SequenceNumber int64  `json:"sequenceNumber,omitempty"`
	Reason         string `json:"reason,omitempty"`
	LinesAdded     int    `json:"linesAdded"`
	LinesRemoved   int    `json:"linesRemoved"`
}

// DiffService records candidate edits as code.diff events.
type DiffService struct{ store DiffStore }

func NewDiffService(store DiffStore) *DiffService { return &DiffService{store: store} }

// Submit validates and records a diff. Out-of-order diffs are dropped:
// they return Accepted false and no error.
func (s *DiffService) Submit(ctx context.Context, session domain.Session, request DiffRequest) (DiffResult, error) {
	interviewID, err := interviewOf(session)
	if err != nil {
		return DiffResult{}, err
	}
	added, removed, err := validateDiff(request)
	if err != nil {
		return DiffResult{}, err
	}
	sequence, err := s.store.RecordDiff(ctx, session.CompanyID, session.ID, interviewID, request.ClientSequence,
		func(sequence int64) (outbox.Message, error) {
			return sessionMessage(session, sequence, events.CodeDiff{
				SessionID: session.ID.String(), InterviewID: interviewID.String(),
				ClientSequence: request.ClientSequence, Origin: request.Origin, PromptID: request.PromptID,
				Path: request.Path, Patch: request.Patch, LinesAdded: added, LinesRemoved: removed,
			})
		})
	if errors.Is(err, ErrDiffOutOfOrder) {
		return DiffResult{Accepted: false, Reason: "out_of_order", LinesAdded: added, LinesRemoved: removed}, nil
	}
	if err != nil {
		return DiffResult{}, fmt.Errorf("record code.diff: %w", err)
	}
	return DiffResult{Accepted: true, SequenceNumber: sequence, LinesAdded: added, LinesRemoved: removed}, nil
}

func validateDiff(request DiffRequest) (int, int, error) {
	invalid := func(format string, args ...any) (int, int, error) {
		return 0, 0, fmt.Errorf("%w: %s", ErrInvalidDiff, fmt.Sprintf(format, args...))
	}
	if request.ClientSequence < 1 {
		return invalid("clientSeq must be positive")
	}
	switch request.Origin {
	case events.DiffOriginManual:
		if request.PromptID != "" {
			return invalid("a manual diff has no promptId")
		}
	case events.DiffOriginAIApplied:
		if request.PromptID != "" {
			if _, err := uuid.Parse(request.PromptID); err != nil {
				return invalid("promptId must be a UUID")
			}
		}
	default:
		return invalid("origin must be manual or ai_applied")
	}
	if err := sandbox.ValidatePath(request.Path); err != nil {
		return invalid("path: %v", err)
	}
	if request.Patch == "" || len(request.Patch) > maxPatchBytes || !utf8.ValidString(request.Patch) {
		return invalid("patch must be non-empty UTF-8 of at most %d bytes", maxPatchBytes)
	}
	if strings.Contains(request.Patch, "\x00") {
		return invalid("patch contains NUL")
	}
	added, removed, err := countUnifiedDiff(request.Patch)
	if err != nil {
		return invalid("patch: %v", err)
	}
	return added, removed, nil
}
