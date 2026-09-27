package byok

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fixture struct {
	secrets  *fakeSecrets
	kms      *fakeKMS
	resolver *Resolver
	clock    *clock
	logs     *bytes.Buffer
}

func newFixture(t *testing.T, config Config) *fixture {
	t.Helper()
	f := &fixture{secrets: &fakeSecrets{}, kms: newFakeKMS(), clock: &clock{now: time.Unix(1_800_000_000, 0)}, logs: &bytes.Buffer{}}
	config.Logger = slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.resolver = NewResolver(f.secrets, f.kms, config)
	f.resolver.now = f.clock.Now
	return f
}

func (f *fixture) store(t *testing.T, company, key, version string) {
	t.Helper()
	stored, err := Seal(context.Background(), f.kms, "alias/byok", company, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	f.secrets.put(f.resolver.SecretName(company), stored, version)
}

func (f *fixture) resolve(t *testing.T, company string) string {
	t.Helper()
	key, err := f.resolver.ResolveKey(context.Background(), company)
	if err != nil {
		t.Fatalf("ResolveKey: %v", err)
	}
	defer key.Zero()
	return key.Reveal()
}

func (f *fixture) cachedKeyBytes(company string) []byte {
	f.resolver.mu.Lock()
	defer f.resolver.mu.Unlock()
	if e, ok := f.resolver.entries[company]; ok {
		return []byte(e.key.Reveal())
	}
	return nil
}

func TestResolveDecryptsInProcessAndCaches(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Minute})
	f.store(t, "c1", "sk-ant-one", "v1")
	if got := f.resolve(t, "c1"); got != "sk-ant-one" {
		t.Fatalf("key = %q", got)
	}
	if got := f.resolve(t, "c1"); got != "sk-ant-one" {
		t.Fatalf("cached key = %q", got)
	}
	if f.kms.decrypts.Load() != 1 || f.secrets.gets.Load() != 1 {
		t.Errorf("KMS decrypts = %d, secret reads = %d; want 1 and 1", f.kms.decrypts.Load(), f.secrets.gets.Load())
	}
	if f.secrets.versionChecks.Load() != 1 {
		t.Errorf("version checks = %d, want 1 (on the cached call)", f.secrets.versionChecks.Load())
	}
}

func TestReturnedKeyIsAPrivateCopy(t *testing.T) {
	f := newFixture(t, Config{})
	f.store(t, "c1", "sk-ant-one", "v1")
	key, err := f.resolver.ResolveKey(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}
	key.Zero()
	if got := f.resolve(t, "c1"); got != "sk-ant-one" {
		t.Errorf("zeroing a returned key corrupted the cache: %q", got)
	}
	f.resolver.Invalidate("c1")
	other, _ := f.resolver.ResolveKey(context.Background(), "c1")
	held, _ := f.resolver.ResolveKey(context.Background(), "c1")
	f.resolver.Invalidate("c1")
	if held.Reveal() != "sk-ant-one" || other.Reveal() != "sk-ant-one" {
		t.Error("evicting the cache wiped a key a caller still holds")
	}
}

func TestTTLExpiryReloadsAndZeroes(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Minute})
	f.store(t, "c1", "sk-ant-one", "v1")
	f.resolve(t, "c1")
	f.resolver.mu.Lock()
	cached := f.resolver.entries["c1"].key
	f.resolver.mu.Unlock()

	f.clock.Advance(61 * time.Second)
	if got := f.resolve(t, "c1"); got != "sk-ant-one" {
		t.Fatalf("after expiry = %q", got)
	}
	if !cached.Zeroed() {
		t.Error("expired key not zeroed")
	}
	if f.kms.decrypts.Load() != 2 {
		t.Errorf("KMS decrypts = %d, want a reload after expiry", f.kms.decrypts.Load())
	}
}

func TestSweepAndCloseZeroIdleKeys(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Minute})
	f.store(t, "c1", "sk-1", "v1")
	f.store(t, "c2", "sk-2", "v1")
	f.resolve(t, "c1")
	f.clock.Advance(30 * time.Second)
	f.resolve(t, "c2")
	f.resolver.mu.Lock()
	k1, k2 := f.resolver.entries["c1"].key, f.resolver.entries["c2"].key
	f.resolver.mu.Unlock()

	f.clock.Advance(31 * time.Second)
	if n := f.resolver.Sweep(); n != 1 || !k1.Zeroed() || k2.Zeroed() {
		t.Errorf("sweep evicted %d; c1 zeroed %v, c2 zeroed %v", n, k1.Zeroed(), k2.Zeroed())
	}
	f.resolver.Close()
	if !k2.Zeroed() || f.cachedKeyBytes("c2") != nil {
		t.Error("Close left a key in memory")
	}
}

func TestRunSweepsAndClosesOnCancel(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	f.store(t, "c1", "sk-1", "v1")
	f.resolve(t, "c1")
	f.resolver.mu.Lock()
	cached := f.resolver.entries["c1"].key
	f.resolver.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.resolver.Run(ctx, time.Millisecond); close(done) }()
	cancel()
	<-done
	if !cached.Zeroed() {
		t.Error("Run did not zero keys on shutdown")
	}
}

func TestRotationIsPickedUpOnTheNextCall(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	f.store(t, "c1", "sk-ant-old", "v1")
	if got := f.resolve(t, "c1"); got != "sk-ant-old" {
		t.Fatal(got)
	}
	f.resolver.mu.Lock()
	old := f.resolver.entries["c1"].key
	f.resolver.mu.Unlock()

	f.store(t, "c1", "sk-ant-new", "v2")
	if got := f.resolve(t, "c1"); got != "sk-ant-new" {
		t.Fatalf("after rotation = %q, want the new key on the very next call", got)
	}
	if !old.Zeroed() {
		t.Error("rotated-out key not zeroed")
	}
}

func TestVersionCheckInterval(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour, VersionCheck: 10 * time.Second})
	f.store(t, "c1", "sk-old", "v1")
	f.resolve(t, "c1")
	f.store(t, "c1", "sk-new", "v2")
	if got := f.resolve(t, "c1"); got != "sk-old" || f.secrets.versionChecks.Load() != 0 {
		t.Errorf("within interval: key %q, checks %d", got, f.secrets.versionChecks.Load())
	}
	f.clock.Advance(11 * time.Second)
	if got := f.resolve(t, "c1"); got != "sk-new" {
		t.Errorf("after interval: key %q", got)
	}
}

func TestInvalidateForcesReload(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	f.store(t, "c1", "sk-old", "v1")
	f.resolve(t, "c1")
	f.resolver.mu.Lock()
	old := f.resolver.entries["c1"].key
	f.resolver.mu.Unlock()
	f.resolver.Invalidate("c1")
	if !old.Zeroed() {
		t.Error("invalidated key not zeroed")
	}
	f.resolve(t, "c1")
	if f.kms.decrypts.Load() != 2 {
		t.Errorf("decrypts = %d, want reload", f.kms.decrypts.Load())
	}
}

func TestMissingAndDeletedKeys(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	if _, err := f.resolver.ResolveKey(context.Background(), "nobody"); !errors.Is(err, ErrNoKey) {
		t.Errorf("missing err = %v", err)
	}
	f.store(t, "c1", "sk", "v1")
	f.resolve(t, "c1")
	f.secrets.remove(f.resolver.SecretName("c1"))
	if _, err := f.resolver.ResolveKey(context.Background(), "c1"); !errors.Is(err, ErrNoKey) {
		t.Errorf("deleted err = %v", err)
	}
	if f.cachedKeyBytes("c1") != nil {
		t.Error("deleted key still cached")
	}
}

func TestKMSFailureIsNotCached(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	f.store(t, "c1", "sk", "v1")
	f.kms.err = errors.New("AccessDeniedException")
	if _, err := f.resolver.ResolveKey(context.Background(), "c1"); err == nil {
		t.Fatal("KMS failure hidden")
	}
	f.kms.err = nil
	if got := f.resolve(t, "c1"); got != "sk" {
		t.Errorf("after recovery = %q", got)
	}
}

func TestConcurrentMissesDecryptOnce(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Hour})
	f.store(t, "c1", "sk-shared", "v1")
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := f.resolver.ResolveKey(context.Background(), "c1")
			if err != nil || key.Reveal() != "sk-shared" {
				t.Errorf("concurrent resolve: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := f.kms.decrypts.Load(); n > 2 {
		t.Errorf("KMS decrypts = %d for 20 concurrent callers", n)
	}
}

func TestResolverNeverLogsKey(t *testing.T) {
	f := newFixture(t, Config{TTL: time.Minute})
	f.store(t, "c1", "sk-ant-LOGME", "v1")
	f.resolve(t, "c1")
	f.store(t, "c1", "sk-ant-LOGME-2", "v2")
	f.resolve(t, "c1")
	f.resolver.Invalidate("c1")
	f.resolver.Close()
	if strings.Contains(f.logs.String(), "LOGME") {
		t.Fatalf("key in logs: %s", f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "byok key loaded") {
		t.Errorf("expected lifecycle logs, got %s", f.logs.String())
	}
}
