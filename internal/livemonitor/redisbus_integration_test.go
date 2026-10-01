//go:build integration

package livemonitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/store/redis"
)

func redisBus(t *testing.T, prefix string, capacity int) *RedisBus {
	t.Helper()
	cfg, _ := config.Load("live-monitor-integration-test")
	client, err := redis.New(cfg.RedisAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("redis: %v", err)
	}
	return NewRedisBus(client.Raw(), prefix, capacity, time.Minute)
}

func TestRedisBusContract(t *testing.T) {
	busContract(t, redisBus(t, "live-test:"+uuid.NewString()+":", 4), 4)
}

func TestRedisBusRingExpires(t *testing.T) {
	cfg, _ := config.Load("live-monitor-integration-test")
	client, _ := redis.New(cfg.RedisAddr)
	defer client.Close()
	bus := NewRedisBus(client.Raw(), "live-test:"+uuid.NewString()+":", 10, 100*time.Millisecond)
	stream := newStream()
	_, _ = bus.Publish(context.Background(), stream, event(stream, 1))
	time.Sleep(250 * time.Millisecond)
	if events, oldest, _ := bus.Recent(context.Background(), stream, 0); len(events) != 0 || oldest != 0 {
		t.Fatalf("ring survived its TTL: %v", seqs(events))
	}
}

// TestTwoReplicasFanOutThroughRedis runs two live-monitor replicas that share
// Redis. Interviewers connected to different replicas both receive events
// ingested by a third, and a reconnect to the other replica replays what was
// missed from the Redis ring.
func TestTwoReplicasFanOutThroughRedis(t *testing.T) {
	prefix := "live-test:" + uuid.NewString() + ":"
	stream := newStream()
	p := rbac.Principal{CompanyID: stream.CompanyID, UserID: uuid.New(), Role: rbac.RoleInterviewer}
	resolver := rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) { return p, nil })

	replicas := make([]*liveFixture, 2)
	for i := range replicas {
		bus := redisBus(t, prefix, 100)
		server := httptest.NewServer(NewHandler(bus, nil, nil, Config{Heartbeat: time.Hour}).Routes(resolver))
		t.Cleanup(server.Close)
		replicas[i] = &liveFixture{server: server, bus: bus, stream: stream, history: &fakeHistory{},
			tokens: map[rbac.Role]string{rbac.RoleInterviewer: "t"}}
	}
	ingest := &liveFixture{bus: redisBus(t, prefix, 100), stream: stream}

	a := replicas[0].connect(t, "t", "")
	b := replicas[1].connect(t, "t", "")
	a.caughtUp(t)
	b.caughtUp(t)
	ingest.publish(t, 1, 3)
	for _, c := range []*liveClient{a, b} {
		for want := int64(1); want <= 3; want++ {
			if got := c.next(t); got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		}
	}

	a.cancel()
	ingest.publish(t, 4, 6)
	again := replicas[1].connect(t, "t", "3")
	if last, replayed := again.caughtUp(t); !equal(replayed, []int64{4, 5, 6}) || last != 6 {
		t.Fatalf("replayed %v at %d", replayed, last)
	}
	ingest.publish(t, 7, 7)
	if got := again.next(t); got != 7 {
		t.Fatalf("live after reconnect = %d", got)
	}
}
