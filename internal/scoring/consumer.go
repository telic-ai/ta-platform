package scoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

type MessageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// Scorer is the part of Service the consumer drives.
type Scorer interface {
	Score(ctx context.Context, trigger Trigger, final bool) (Outcome, error)
}

type ConsumerConfig struct {
	// MaxAttempts is how many times a trigger is tried while the event log
	// is behind; the last attempt scores whatever the log holds.
	MaxAttempts int
	// BaseBackoff doubles after each behind attempt, up to MaxBackoff.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// OnInvalid is called for a message that is not a valid trigger or that
	// failed permanently; it is committed and skipped.
	OnInvalid func(kafkago.Message, error)
	// OnScored is called after each trigger is handled.
	OnScored func(Trigger, Outcome, int)
}

// Consumer reads session events and scores each session-ending trigger.
type Consumer struct {
	reader MessageReader
	scorer Scorer
	config ConsumerConfig
	sleep  func(context.Context, time.Duration) error
}

func NewConsumer(reader MessageReader, scorer Scorer, config ConsumerConfig) (*Consumer, error) {
	if reader == nil || scorer == nil {
		return nil, errors.New("scoring: reader and scorer are required")
	}
	if config.MaxAttempts < 1 || config.BaseBackoff <= 0 || config.MaxBackoff < config.BaseBackoff {
		return nil, errors.New("scoring: max attempts and backoff must be positive, with max backoff >= base")
	}
	return &Consumer{reader: reader, scorer: scorer, config: config, sleep: sleepContext}, nil
}

func (c *Consumer) Close() error { return c.reader.Close() }

// Run consumes until ctx is done. A trigger's offset is committed only once
// it has been scored (or found already scored), so a crash re-scores it,
// which is a no-op. While the event log is behind, the trigger is not
// acknowledged: it is retried in place with exponential backoff, which also
// holds back later events on its partition.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("scoring: fetch event: %w", err)
		}
		if err := c.handle(ctx, message); err != nil {
			return err
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("scoring: commit offset: %w", err)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, message kafkago.Message) error {
	var envelope events.Envelope
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		c.invalid(message, fmt.Errorf("envelope: %w", err))
		return nil
	}
	trigger, ok, err := ParseTrigger(envelope)
	if err != nil {
		c.invalid(message, err)
		return nil
	}
	if !ok {
		return nil
	}
	for attempt := 1; ; attempt++ {
		outcome, err := c.scorer.Score(ctx, trigger, attempt >= c.config.MaxAttempts)
		var permanent *PermanentError
		switch {
		case err == nil:
			if c.config.OnScored != nil {
				c.config.OnScored(trigger, outcome, attempt)
			}
			return nil
		case errors.Is(err, ErrBehind):
			if err := c.sleep(ctx, c.Backoff(attempt)); err != nil {
				return err
			}
		case errors.As(err, &permanent):
			c.invalid(message, err)
			return nil
		default:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

// Backoff is the wait after the given failed attempt (1-based).
func (c *Consumer) Backoff(attempt int) time.Duration {
	wait := c.config.BaseBackoff
	for i := 1; i < attempt && wait < c.config.MaxBackoff; i++ {
		wait *= 2
	}
	return min(wait, c.config.MaxBackoff)
}

func (c *Consumer) invalid(message kafkago.Message, err error) {
	if c.config.OnInvalid != nil {
		c.config.OnInvalid(message, err)
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
