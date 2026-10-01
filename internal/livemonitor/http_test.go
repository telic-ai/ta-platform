package livemonitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/sse"
)

// viewers maps bearer tokens to principals.
type viewers map[string]rbac.Principal

func (v viewers) Resolve(r *http.Request) (rbac.Principal, error) {
	token, _ := auth.BearerToken(r)
	p, ok := v[token]
	if !ok {
		return rbac.Principal{}, rbac.ErrUnauthenticated
	}
	return p, nil
}

type fakeHistory struct {
	events []Event
	err    error
	calls  int
}

func (h *fakeHistory) Timeline(_ context.Context, companyID, interviewID uuid.UUID, after int64) ([]Event, error) {
	h.calls++
	var out []Event
	for _, e := range h.events {
		if e.CompanyID == companyID.String() && e.InterviewID == interviewID.String() && e.SequenceNumber > after {
			out = append(out, e)
		}
	}
	return out, h.err
}

type accessFunc func(uuid.UUID, uuid.UUID) error

func (f accessFunc) LiveMonitoring(_ context.Context, companyID, interviewID uuid.UUID) error {
	return f(companyID, interviewID)
}

type liveFixture struct {
	server  *httptest.Server
	bus     Bus
	stream  Stream
	history *fakeHistory
	tokens  map[rbac.Role]string
	otherCo string
}

func newLiveFixture(t *testing.T, bus Bus, access Access) *liveFixture {
	t.Helper()
	f := &liveFixture{bus: bus, stream: newStream(), history: &fakeHistory{}, tokens: map[rbac.Role]string{}}
	v := viewers{}
	for _, role := range rbac.Roles {
		token := "tok-" + string(role)
		f.tokens[role] = token
		v[token] = rbac.Principal{CompanyID: f.stream.CompanyID, UserID: uuid.New(), Role: role}
	}
	f.otherCo = "tok-other-company"
	v[f.otherCo] = rbac.Principal{CompanyID: uuid.New(), UserID: uuid.New(), Role: rbac.RoleOwner}
	handler := NewHandler(bus, f.history, access, Config{Heartbeat: time.Hour})
	f.server = httptest.NewServer(handler.Routes(v))
	t.Cleanup(f.server.Close)
	return f
}

// liveClient is one open SSE connection.
type liveClient struct {
	resp   *http.Response
	events chan sse.Event
	cancel context.CancelFunc
}

func (f *liveFixture) connect(t *testing.T, token string, lastEventID string) *liveClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", f.server.URL+"/interviews/"+f.stream.InterviewID.String()+"/live", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	c := &liveClient{resp: resp, events: make(chan sse.Event, 1000), cancel: cancel}
	t.Cleanup(func() { cancel(); _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		return c
	}
	go func() {
		defer close(c.events)
		reader := sse.NewReader(resp.Body)
		for {
			e, err := reader.Next()
			if err != nil {
				return
			}
			c.events <- e
		}
	}()
	return c
}

// next returns the next "event" event's sequence number, skipping others.
func (c *liveClient) next(t *testing.T) int64 {
	t.Helper()
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatal("stream closed")
			}
			if e.Name != "event" {
				continue
			}
			var body Event
			if err := json.Unmarshal(e.Data, &body); err != nil {
				t.Fatal(err)
			}
			if strconv.FormatInt(body.SequenceNumber, 10) != e.ID {
				t.Fatalf("id %q does not match sequence %d", e.ID, body.SequenceNumber)
			}
			return body.SequenceNumber
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for an event")
		}
	}
}

// caughtUp waits for the caught_up marker and returns its last_event_id.
func (c *liveClient) caughtUp(t *testing.T) (int64, []int64) {
	t.Helper()
	var before []int64
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatal("stream closed")
			}
			switch e.Name {
			case "event":
				var body Event
				_ = json.Unmarshal(e.Data, &body)
				before = append(before, body.SequenceNumber)
			case "caught_up":
				var body map[string]int64
				_ = json.Unmarshal(e.Data, &body)
				return body["last_event_id"], before
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for caught_up")
		}
	}
}

func (f *liveFixture) publish(t *testing.T, from, to int64) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		if _, err := f.bus.Publish(context.Background(), f.stream, event(f.stream, seq)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTwoInterviewersBothReceiveFanOut(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(100), nil)
	a := f.connect(t, f.tokens[rbac.RoleInterviewer], "")
	b := f.connect(t, f.tokens[rbac.RoleRecruiter], "")
	a.caughtUp(t)
	b.caughtUp(t)
	f.publish(t, 1, 3)
	for _, c := range []*liveClient{a, b} {
		for want := int64(1); want <= 3; want++ {
			if got := c.next(t); got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		}
	}
}

func TestReconnectReplaysMissedEventsFromRing(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(100), nil)
	first := f.connect(t, f.tokens[rbac.RoleInterviewer], "")
	first.caughtUp(t)
	f.publish(t, 1, 2)
	first.next(t)
	if last := first.next(t); last != 2 {
		t.Fatalf("last = %d", last)
	}
	first.cancel() // disconnect

	f.publish(t, 3, 5)  // missed while away
	f.history.calls = 0 // the first connection's empty ring read history

	second := f.connect(t, f.tokens[rbac.RoleInterviewer], "2")
	last, replayed := second.caughtUp(t)
	if !equal(replayed, []int64{3, 4, 5}) || last != 5 {
		t.Fatalf("replayed %v, caught up at %d", replayed, last)
	}
	if f.history.calls != 0 {
		t.Fatal("ring covered the gap but history was read")
	}
	f.publish(t, 6, 6)
	if got := second.next(t); got != 6 {
		t.Fatalf("live after replay = %d", got)
	}
}

func TestFreshViewerGetsFullHistoryThenLive(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(100), nil)
	f.publish(t, 1, 3)
	c := f.connect(t, f.tokens[rbac.RoleOwner], "")
	if last, replayed := c.caughtUp(t); !equal(replayed, []int64{1, 2, 3}) || last != 3 {
		t.Fatalf("replayed %v at %d", replayed, last)
	}
}

func TestDeepFallbackReadsHistoryBeyondTheRing(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(2), nil)
	for seq := int64(1); seq <= 5; seq++ {
		f.history.events = append(f.history.events, event(f.stream, seq))
	}
	f.publish(t, 1, 5) // ring keeps 4, 5

	c := f.connect(t, f.tokens[rbac.RoleInterviewer], "1")
	if last, replayed := c.caughtUp(t); !equal(replayed, []int64{2, 3, 4, 5}) || last != 5 {
		t.Fatalf("replayed %v at %d", replayed, last)
	}
	if f.history.calls != 1 {
		t.Fatalf("history calls = %d", f.history.calls)
	}
}

func TestDeepFallbackWhenRingExpired(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(10), nil)
	for seq := int64(1); seq <= 3; seq++ {
		f.history.events = append(f.history.events, event(f.stream, seq))
	}
	c := f.connect(t, f.tokens[rbac.RoleInterviewer], "1")
	if _, replayed := c.caughtUp(t); !equal(replayed, []int64{2, 3}) {
		t.Fatalf("replayed %v", replayed)
	}
}

func TestHistoryFailureDegradesToRing(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(2), nil)
	f.history.err = errors.New("clickhouse down")
	f.publish(t, 1, 5)
	c := f.connect(t, f.tokens[rbac.RoleInterviewer], "1")
	if _, replayed := c.caughtUp(t); !equal(replayed, []int64{4, 5}) {
		t.Fatalf("replayed %v", replayed)
	}
}

// droppingBus delivers only the events listed in deliver to subscribers,
// simulating pub/sub loss; the ring still gets everything.
type droppingBus struct {
	*MemoryBus
	subs []chan Event
}

type chanSub struct{ ch chan Event }

func (s chanSub) Events() <-chan Event { return s.ch }
func (s chanSub) Close() error         { return nil }

func (b *droppingBus) Subscribe(context.Context, Stream) (Subscription, error) {
	ch := make(chan Event, 10)
	b.subs = append(b.subs, ch)
	return chanSub{ch}, nil
}

func TestLiveGapIsRepairedFromRing(t *testing.T) {
	bus := &droppingBus{MemoryBus: NewMemoryBus(100)}
	f := newLiveFixture(t, bus, nil)
	c := f.connect(t, f.tokens[rbac.RoleInterviewer], "")
	c.caughtUp(t)
	f.publish(t, 1, 3)                // ring only; pub/sub "lost" them
	bus.subs[0] <- event(f.stream, 3) // only 3 arrives live
	for want := int64(1); want <= 3; want++ {
		if got := c.next(t); got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	}
	// Duplicates from pub/sub are dropped.
	bus.subs[0] <- event(f.stream, 2)
	f.publish(t, 4, 4)
	bus.subs[0] <- event(f.stream, 4)
	if got := c.next(t); got != 4 {
		t.Fatalf("got %d after duplicate", got)
	}
}

func TestStreamIgnoresForeignEvents(t *testing.T) {
	bus := &droppingBus{MemoryBus: NewMemoryBus(100)}
	f := newLiveFixture(t, bus, nil)
	c := f.connect(t, f.tokens[rbac.RoleInterviewer], "")
	c.caughtUp(t)
	foreign := event(Stream{CompanyID: uuid.New(), InterviewID: f.stream.InterviewID}, 1)
	bus.subs[0] <- foreign
	f.publish(t, 1, 1)
	bus.subs[0] <- event(f.stream, 1)
	if got := c.next(t); got != 1 {
		t.Fatalf("got %d", got)
	}
	select {
	case e := <-c.events:
		t.Fatalf("unexpected event %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestOtherCompanyCannotWatch(t *testing.T) {
	f := newLiveFixture(t, NewMemoryBus(100), nil)
	f.publish(t, 1, 3)
	c := f.connect(t, f.otherCo, "")
	if _, replayed := c.caughtUp(t); len(replayed) != 0 {
		t.Fatalf("other company saw %v", replayed)
	}
	f.publish(t, 4, 4)
	select {
	case e := <-c.events:
		t.Fatalf("other company received %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLiveRejections(t *testing.T) {
	cases := map[string]struct {
		access Access
		token  func(*liveFixture) string
		path   func(*liveFixture) string
		header string
		status int
	}{
		"no token":       {nil, func(*liveFixture) string { return "" }, nil, "", 401},
		"viewer role":    {nil, func(f *liveFixture) string { return f.tokens[rbac.RoleViewer] }, nil, "", 403},
		"bad id":         {nil, nil, func(f *liveFixture) string { return "/interviews/nope/live" }, "", 400},
		"bad last id":    {nil, nil, nil, "-1", 400},
		"not found":      {accessFunc(func(uuid.UUID, uuid.UUID) error { return ErrInterviewNotFound }), nil, nil, "", 404},
		"disabled":       {accessFunc(func(uuid.UUID, uuid.UUID) error { return ErrMonitoringDisabled }), nil, nil, "", 403},
		"access failure": {accessFunc(func(uuid.UUID, uuid.UUID) error { return errors.New("db") }), nil, nil, "", 503},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newLiveFixture(t, NewMemoryBus(10), tc.access)
			token := f.tokens[rbac.RoleInterviewer]
			if tc.token != nil {
				token = tc.token(f)
			}
			path := "/interviews/" + f.stream.InterviewID.String() + "/live"
			if tc.path != nil {
				path = tc.path(f)
			}
			req, _ := http.NewRequest("GET", f.server.URL+path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			if tc.header != "" {
				req.Header.Set("Last-Event-ID", tc.header)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}
}

func TestAccessIsCheckedWithPrincipalsCompany(t *testing.T) {
	var gotCompany, gotInterview uuid.UUID
	f := newLiveFixture(t, NewMemoryBus(10), accessFunc(func(c, i uuid.UUID) error { gotCompany, gotInterview = c, i; return nil }))
	c := f.connect(t, f.tokens[rbac.RoleAdmin], "")
	c.caughtUp(t)
	if gotCompany != f.stream.CompanyID || gotInterview != f.stream.InterviewID {
		t.Fatalf("access checked for %s/%s", gotCompany, gotInterview)
	}
}

func TestHeartbeat(t *testing.T) {
	bus, stream := NewMemoryBus(10), newStream()
	p := rbac.Principal{CompanyID: stream.CompanyID, UserID: uuid.New(), Role: rbac.RoleInterviewer}
	server := httptest.NewServer(NewHandler(bus, nil, nil, Config{Heartbeat: 10 * time.Millisecond}).
		Routes(rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) { return p, nil })))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/interviews/"+stream.InterviewID.String()+"/live", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 4096)
	var seen strings.Builder
	for !strings.Contains(seen.String(), ": ping\n") {
		n, err := resp.Body.Read(buf)
		if err != nil {
			t.Fatalf("read: %v (saw %q)", err, seen.String())
		}
		seen.Write(buf[:n])
	}
}

func TestLastEventID(t *testing.T) {
	r := httptest.NewRequest("GET", "/?last_event_id=9", nil)
	if v, err := LastEventID(r); err != nil || v != 9 {
		t.Fatalf("query = %d %v", v, err)
	}
	r.Header.Set("Last-Event-ID", "12")
	if v, err := LastEventID(r); err != nil || v != 12 {
		t.Fatalf("header wins = %d %v", v, err)
	}
	if v, err := LastEventID(httptest.NewRequest("GET", "/", nil)); err != nil || v != 0 {
		t.Fatalf("absent = %d %v", v, err)
	}
	for _, bad := range []string{"-1", "x", "1.5"} {
		r := httptest.NewRequest("GET", "/?last_event_id="+url.QueryEscape(bad), nil)
		if _, err := LastEventID(r); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
