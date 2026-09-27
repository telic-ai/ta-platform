package candidateworkspace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

const (
	maxPromptBytes   = 32 << 10
	maxHistoryTurns  = 50
	maxHistoryBytes  = 512 << 10
	promptEventCount = 2 // prompt.submitted + the reserved ai.response.completed
)

// ErrInvalidPrompt is returned for prompts rejected before any side effect.
var ErrInvalidPrompt = errors.New("invalid prompt")

// Completer streams a completion from the AI Gateway.
type Completer interface {
	Complete(ctx context.Context, request aigateway.CompleteRequest, onDelta func(string) error) (aigateway.Outcome, error)
}

// PromptRequest is the body of POST /session/prompt.
type PromptRequest struct {
	Prompt  string              `json:"prompt"`
	History []aigateway.Message `json:"history,omitempty"`
}

func (r PromptRequest) validate() error {
	if strings.TrimSpace(r.Prompt) == "" || len(r.Prompt) > maxPromptBytes || !utf8.ValidString(r.Prompt) {
		return fmt.Errorf("%w: prompt must be non-empty UTF-8 of at most %d bytes", ErrInvalidPrompt, maxPromptBytes)
	}
	if len(r.History) > maxHistoryTurns {
		return fmt.Errorf("%w: at most %d history turns", ErrInvalidPrompt, maxHistoryTurns)
	}
	size := 0
	for i, turn := range r.History {
		if turn.Role != aigateway.RoleUser && turn.Role != aigateway.RoleAssistant {
			return fmt.Errorf("%w: history[%d].role must be user or assistant", ErrInvalidPrompt, i)
		}
		if strings.TrimSpace(turn.Content) == "" {
			return fmt.Errorf("%w: history[%d].content must not be empty", ErrInvalidPrompt, i)
		}
		size += len(turn.Content)
	}
	if size > maxHistoryBytes {
		return fmt.Errorf("%w: history exceeds %d bytes", ErrInvalidPrompt, maxHistoryBytes)
	}
	return nil
}

// PromptAccepted is sent to the client before any model output.
type PromptAccepted struct {
	PromptID       string `json:"promptId"`
	SequenceNumber int64  `json:"sequenceNumber"`
}

// PromptConfig sets what the workspace asks the gateway for.
type PromptConfig struct {
	Model     string
	System    string
	MaxTokens int64
	// Mode returns the gateway mode (managed or byok) for a company. Nil
	// means managed for everyone.
	Mode func(ctx context.Context, companyID uuid.UUID) (string, error)
}

// PromptService runs the candidate prompt flow.
type PromptService struct {
	store   EventStore
	gateway Completer
	config  PromptConfig
}

func NewPromptService(store EventStore, gateway Completer, config PromptConfig) *PromptService {
	return &PromptService{store: store, gateway: gateway, config: config}
}

// Submit records prompt.submitted (allocating its sequence number and
// reserving the next one for the completion) before calling the gateway,
// then streams the completion. accepted is called once the prompt is
// durably recorded; onDelta for each model fragment. Cancelling ctx (the
// client disconnecting) cancels the upstream call.
func (s *PromptService) Submit(ctx context.Context, session domain.Session, request PromptRequest,
	accepted func(PromptAccepted) error, onDelta func(string) error) (aigateway.Outcome, error) {
	if err := request.validate(); err != nil {
		return aigateway.Outcome{}, err
	}
	interviewID, err := interviewOf(session)
	if err != nil {
		return aigateway.Outcome{}, err
	}
	mode := aigateway.ModeManaged
	if s.config.Mode != nil {
		if mode, err = s.config.Mode(ctx, session.CompanyID); err != nil {
			return aigateway.Outcome{}, fmt.Errorf("resolve AI mode: %w", err)
		}
	}

	promptID := uuid.New()
	first, err := s.store.RecordEvents(ctx, session.CompanyID, interviewID, promptEventCount, func(first int64) ([]outbox.Message, error) {
		message, err := sessionMessage(session, first, events.PromptSubmitted{
			SessionID: session.ID.String(), InterviewID: interviewID.String(),
			PromptID: promptID.String(), Prompt: request.Prompt,
			HistoryTurns: len(request.History), CompletionSequenceNumber: first + 1,
		})
		return []outbox.Message{message}, err
	})
	if err != nil {
		return aigateway.Outcome{}, fmt.Errorf("record prompt.submitted: %w", err)
	}
	if err := accepted(PromptAccepted{PromptID: promptID.String(), SequenceNumber: first}); err != nil {
		return aigateway.Outcome{}, err
	}

	messages := append(append([]aigateway.Message{}, request.History...),
		aigateway.Message{Role: aigateway.RoleUser, Content: request.Prompt})
	return s.gateway.Complete(ctx, aigateway.CompleteRequest{
		CompanyID: session.CompanyID.String(), InterviewID: interviewID.String(),
		SessionID: session.ID.String(), PromptID: promptID.String(),
		SequenceNumber: first + 1, Mode: mode,
		Model: s.config.Model, System: s.config.System, MaxTokens: s.config.MaxTokens,
		Messages: messages,
	}, onDelta)
}
