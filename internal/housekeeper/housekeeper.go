// Package housekeeper purges interviews whose retention period has ended or
// whose candidate asked to be erased, unless they are under legal hold.
//
// A purge deletes the interview's documents from Typesense, then its rows
// from ClickHouse, then skeletonizes it in Postgres (personal data removed,
// ids and lifecycle timestamps kept) and writes purge_log, all while holding
// the interview's row lock. S3 objects expire through bucket lifecycle rules
// and Kafka records through topic retention, so the Housekeeper has no
// client for either.
//
// Every step is idempotent and Postgres commits last: a run that fails
// midway leaves purged_at unset, and the next run repeats the deletes
// (deleting nothing new) and finishes the purge.
//
// NOTE: [[Housekeeper]] was not available when this package was written. Its
// Postgres deletion list names an ai_calls table, which does not exist (AI
// completions are recorded only as ai.response.completed events), so it is
// deliberately absent here.
package housekeeper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Purge reasons, as recorded in purge_log.
const (
	ReasonRetention = "retention"
	ReasonErasure   = "erasure"
)

// Candidate is an interview eligible for purging.
type Candidate struct {
	CompanyID   uuid.UUID
	InterviewID uuid.UUID
	Reason      string
}

// Stats are what a purge's external deletes removed.
type Stats struct {
	SearchDocumentsDeleted int
}

// Store is the Postgres side of the Housekeeper.
type Store interface {
	// Candidates lists up to limit purge candidates as of now: not purged,
	// not under legal hold, and either erasure-requested or terminal before
	// retentionCutoff.
	Candidates(ctx context.Context, retentionCutoff time.Time, limit int) ([]Candidate, error)
	// Purge locks the interview, re-checks that it is still a candidate, runs
	// deleteExternal, and then skeletonizes it and writes purge_log in the
	// same transaction. purged is false, and deleteExternal is not called,
	// when the interview is no longer a candidate (already purged, or put on
	// legal hold since it was listed).
	Purge(ctx context.Context, candidate Candidate, retentionCutoff time.Time, deleteExternal func(context.Context) (Stats, error)) (purged bool, err error)
}

// Search deletes an interview's indexed documents.
type Search interface {
	DeleteInterview(ctx context.Context, companyID, interviewID uuid.UUID) (int, error)
}

// Analytics deletes an interview's rows from the ClickHouse event log and
// its per-session projections.
type Analytics interface {
	DeleteInterview(ctx context.Context, companyID, interviewID uuid.UUID) error
}

type Config struct {
	// Retention is how long a terminal interview is kept.
	Retention time.Duration
	// BatchSize caps the candidates handled per run.
	BatchSize int
}

type Housekeeper struct {
	store     Store
	search    Search
	analytics Analytics
	config    Config
	now       func() time.Time
}

func New(store Store, search Search, analytics Analytics, config Config) (*Housekeeper, error) {
	if store == nil || search == nil || analytics == nil {
		return nil, errors.New("housekeeper: store, search and analytics are required")
	}
	if config.Retention <= 0 || config.BatchSize <= 0 {
		return nil, errors.New("housekeeper: retention and batch size must be positive")
	}
	return &Housekeeper{store: store, search: search, analytics: analytics, config: config, now: time.Now}, nil
}

// Failure is one interview the run could not purge.
type Failure struct {
	Candidate Candidate
	Err       error
}

// Report summarizes a run.
type Report struct {
	Candidates int
	Purged     []Candidate
	Skipped    []Candidate
	Failures   []Failure
}

// Run purges one batch of candidates. One interview's failure does not stop
// the others; it is reported and retried on the next run. The error is
// non-nil only if candidates could not be listed.
func (h *Housekeeper) Run(ctx context.Context) (Report, error) {
	cutoff := h.now().UTC().Add(-h.config.Retention)
	candidates, err := h.store.Candidates(ctx, cutoff, h.config.BatchSize)
	if err != nil {
		return Report{}, fmt.Errorf("housekeeper: list candidates: %w", err)
	}
	report := Report{Candidates: len(candidates)}
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		purged, err := h.store.Purge(ctx, candidate, cutoff, func(ctx context.Context) (Stats, error) {
			return h.deleteExternal(ctx, candidate)
		})
		switch {
		case err != nil:
			report.Failures = append(report.Failures, Failure{Candidate: candidate, Err: err})
		case purged:
			report.Purged = append(report.Purged, candidate)
		default:
			report.Skipped = append(report.Skipped, candidate)
		}
	}
	return report, nil
}

// deleteExternal removes the interview from Typesense, then ClickHouse.
func (h *Housekeeper) deleteExternal(ctx context.Context, candidate Candidate) (Stats, error) {
	deleted, err := h.search.DeleteInterview(ctx, candidate.CompanyID, candidate.InterviewID)
	if err != nil {
		return Stats{}, fmt.Errorf("delete search documents: %w", err)
	}
	if err := h.analytics.DeleteInterview(ctx, candidate.CompanyID, candidate.InterviewID); err != nil {
		return Stats{}, fmt.Errorf("delete event log rows: %w", err)
	}
	return Stats{SearchDocumentsDeleted: deleted}, nil
}
