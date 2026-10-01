//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/livemonitor"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/replay"
	"github.com/telic-ai/ta-platform/internal/sse"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/redis"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

// liveViewer is one interviewer's open /live stream.
type liveViewer struct {
	events chan sse.Event
	cancel context.CancelFunc
}

func watchLive(t *testing.T, ctx context.Context, baseURL, token string, interviewID uuid.UUID, lastEventID string) *liveViewer {
	t.Helper()
	streamCtx, cancel := context.WithCancel(ctx)
	req, _ := http.NewRequestWithContext(streamCtx, "GET", baseURL+"/interviews/"+interviewID.String()+"/live", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		resp.Body.Close()
		t.Fatalf("live status = %d", resp.StatusCode)
	}
	v := &liveViewer{events: make(chan sse.Event, 100), cancel: cancel}
	t.Cleanup(func() { cancel(); resp.Body.Close() })
	go func() {
		defer close(v.events)
		reader := sse.NewReader(resp.Body)
		for {
			e, err := reader.Next()
			if err != nil {
				return
			}
			v.events <- e
		}
	}()
	return v
}

// sequences collects the sequence numbers of "event" events until n have
// arrived.
func (v *liveViewer) sequences(t *testing.T, n int) []int64 {
	t.Helper()
	var out []int64
	timeout := time.After(20 * time.Second)
	for len(out) < n {
		select {
		case e, ok := <-v.events:
			if !ok {
				t.Fatalf("stream closed after %v", out)
			}
			if e.Name != "event" {
				continue
			}
			var body livemonitor.Event
			if err := json.Unmarshal(e.Data, &body); err != nil {
				t.Fatal(err)
			}
			out = append(out, body.SequenceNumber)
		case <-timeout:
			t.Fatalf("timed out after %v", out)
		}
	}
	return out
}

func (v *liveViewer) waitCaughtUp(t *testing.T) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e := <-v.events:
			if e.Name == "caught_up" {
				return
			}
		case <-timeout:
			t.Fatal("no caught_up")
		}
	}
}

func sameSeqs(got []int64, from, to int64) bool {
	if int64(len(got)) != to-from+1 {
		return false
	}
	for i, seq := range got {
		if seq != from+int64(i) {
			return false
		}
	}
	return true
}

// TestLiveMonitoringEndToEnd: candidate events flow Kafka -> ingester ->
// Redis ring + pub/sub -> SSE. Two interviewers on the same interview both
// receive them; a reconnect replays missed events from the ring, and one
// further behind than the ring reaches is filled from ClickHouse.
func TestLiveMonitoringEndToEnd(t *testing.T) {
	cfg, _ := config.Load("live-monitor-e2e-test")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	candidate := seedCandidateSession(t, ctx, pool)
	_, interviewerA := seedMember(t, ctx, pool, candidate.CompanyID, rbac.RoleInterviewer)
	_, interviewerB := seedMember(t, ctx, pool, candidate.CompanyID, rbac.RoleRecruiter)
	_, viewer := seedMember(t, ctx, pool, candidate.CompanyID, rbac.RoleViewer)

	kafkaClient := kafka.New(cfg.KafkaBrokers)
	topic := isolatedTopic(t, ctx, kafkaClient, "live")

	redisClient, err := redis.New(cfg.RedisAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()
	// A small ring makes the ClickHouse fallback reachable.
	const ringSize = 3
	bus := livemonitor.NewRedisBus(redisClient.Raw(), "live-e2e:"+uuid.NewString()+":", ringSize, time.Minute)

	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseClient.Close()
	eventStore, err := eventlogwriter.NewStore(clickhouseClient.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if err := eventStore.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// The live ingester and the event-log-writer consume the same topic in
	// their own groups, as in production.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	liveReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{cfg.KafkaBrokers}, Topic: topic,
		GroupID: "live-" + topic, StartOffset: kafkago.FirstOffset, MaxWait: 50 * time.Millisecond})
	defer liveReader.Close()
	ingestDone := make(chan error, 1)
	go func() { ingestDone <- livemonitor.NewIngester(liveReader, bus).Run(runCtx) }()
	logReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{cfg.KafkaBrokers}, Topic: topic,
		GroupID: "log-" + topic, StartOffset: kafkago.FirstOffset, MaxWait: 50 * time.Millisecond})
	logWriter, err := eventlogwriter.New(logReader, eventStore, eventlogwriter.Config{BatchSize: 50, BatchWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer logWriter.Close()
	logDone := make(chan error, 1)
	go func() { logDone <- logWriter.Run(runCtx) }()
	defer func() {
		stopRun()
		for name, done := range map[string]chan error{"ingester": ingestDone, "event-log-writer": logDone} {
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("%s: %v", name, err)
			}
		}
	}()

	store := postgres.NewAdminStore(pool)
	history := replay.NewClickHouseStore(clickhouseClient.Conn())
	server := httptest.NewServer(livemonitor.NewHandler(bus, history, store, livemonitor.Config{}).
		Routes(rbac.Resolver{Sessions: postgres.NewSessionStore(pool), Roles: store}))
	// A cleanup, not a defer: open streams are cancelled by their own
	// cleanups, which run first.
	t.Cleanup(server.Close)

	// The candidate's session emits events; the workspace's outbox relay
	// would publish them keyed by session.
	producer := kafkaClient.Writer(topic)
	defer producer.Close()
	emit := func(from, to int64) {
		t.Helper()
		var messages []kafkago.Message
		for seq := from; seq <= to; seq++ {
			envelope, err := events.New(candidate.CompanyID.String(), seq, events.CodeDiff{
				SessionID: candidate.SessionID.String(), InterviewID: candidate.InterviewID.String(),
				ClientSequence: seq, Origin: events.DiffOriginManual, Path: "main.go", Patch: "@@ -0,0 +1 @@\n+x\n", LinesAdded: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			value, _ := json.Marshal(envelope)
			messages = append(messages, kafkago.Message{Key: []byte(candidate.SessionID.String()), Value: value})
		}
		if err := producer.WriteMessages(ctx, messages...); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Viewers cannot watch; candidates are not members.
	for _, token := range []string{viewer, candidate.Token} {
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/interviews/"+candidate.InterviewID.String()+"/live", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q watched live: %d", token, resp.StatusCode)
		}
	}

	a := watchLive(t, ctx, server.URL, interviewerA, candidate.InterviewID, "")
	b := watchLive(t, ctx, server.URL, interviewerB, candidate.InterviewID, "")
	a.waitCaughtUp(t)
	b.waitCaughtUp(t)

	emit(1, 3)
	for name, v := range map[string]*liveViewer{"A": a, "B": b} {
		if got := v.sequences(t, 3); !sameSeqs(got, 1, 3) {
			t.Fatalf("interviewer %s got %v", name, got)
		}
	}

	// A drops off; two events arrive while it is away. The ring (size 3)
	// still holds them, so the reconnect replays from Redis.
	a.cancel()
	emit(4, 5)
	if got := b.sequences(t, 2); !sameSeqs(got, 4, 5) {
		t.Fatalf("B got %v", got)
	}
	a = watchLive(t, ctx, server.URL, interviewerA, candidate.InterviewID, "3")
	if got := a.sequences(t, 2); !sameSeqs(got, 4, 5) {
		t.Fatalf("A's ring replay = %v", got)
	}
	a.waitCaughtUp(t)

	// B drops off for longer than the ring covers: 6..10 arrive, the ring
	// keeps 8..10, and 6..7 must come from ClickHouse.
	b.cancel()
	emit(6, 10)
	if got := a.sequences(t, 5); !sameSeqs(got, 6, 10) {
		t.Fatalf("A got %v", got)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		logged, err := history.Timeline(ctx, candidate.CompanyID, candidate.InterviewID, 0)
		if err == nil && len(logged) >= 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ClickHouse holds %d events: %v", len(logged), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	b = watchLive(t, ctx, server.URL, interviewerB, candidate.InterviewID, "5")
	if got := b.sequences(t, 5); !sameSeqs(got, 6, 10) {
		t.Fatalf("B's deep replay = %v", got)
	}
	b.waitCaughtUp(t)

	// Both are live again.
	emit(11, 11)
	for name, v := range map[string]*liveViewer{"A": a, "B": b} {
		if got := v.sequences(t, 1); !sameSeqs(got, 11, 11) {
			t.Fatalf("interviewer %s got %v", name, got)
		}
	}
}
