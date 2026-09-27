package byok

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/telic-ai/ta-platform/internal/aigateway"
)

// ErrNoKey means the company has no provider key stored.
var ErrNoKey = errors.New("byok: no provider key for company")

// Secret is one version of a secret's value.
type Secret struct {
	Value     []byte
	VersionID string
}

// Secrets is the part of AWS Secrets Manager the resolver uses.
type Secrets interface {
	// Get returns the current version of the secret. It returns ErrNoKey
	// if the secret does not exist.
	Get(ctx context.Context, name string) (Secret, error)
	// CurrentVersion returns the current version's ID without the value.
	CurrentVersion(ctx context.Context, name string) (string, error)
}

// Config tunes the resolver.
type Config struct {
	// SecretPrefix + company ID names each company's secret.
	SecretPrefix string
	// TTL bounds how long a decrypted key stays in memory.
	TTL time.Duration
	// VersionCheck is how often a cached key's secret version is compared
	// with the current one. Zero checks on every call, so a rotation is
	// picked up by the very next completion.
	VersionCheck time.Duration
	Logger       *slog.Logger
}

type entry struct {
	key       *aigateway.Key
	version   string
	expires   time.Time
	checkedAt time.Time
}

// Resolver implements aigateway.KeyResolver with a TTL cache of decrypted
// keys. Every returned key is a private copy the caller must Zero after
// use; the cache's own copy is zeroed when evicted, expired, rotated or
// invalidated, and never handed out.
type Resolver struct {
	secrets Secrets
	kms     KMS
	config  Config
	now     func() time.Time

	mu      sync.Mutex
	entries map[string]*entry
	// loads collapses concurrent misses for a company into one KMS call.
	loads map[string]*load
}

type load struct {
	done    chan struct{}
	key     *aigateway.Key
	version string
	err     error
}

func NewResolver(secrets Secrets, kms KMS, config Config) *Resolver {
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.SecretPrefix == "" {
		config.SecretPrefix = "ta-platform/companies/"
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Resolver{secrets: secrets, kms: kms, config: config, now: time.Now,
		entries: map[string]*entry{}, loads: map[string]*load{}}
}

// SecretName is where companyID's key is stored.
func (r *Resolver) SecretName(companyID string) string {
	return r.config.SecretPrefix + companyID + "/ai-provider-key"
}

// ResolveKey returns a copy of companyID's decrypted key.
func (r *Resolver) ResolveKey(ctx context.Context, companyID string) (*aigateway.Key, error) {
	now := r.now()
	r.mu.Lock()
	cached, ok := r.entries[companyID]
	if ok && !now.Before(cached.expires) {
		r.evictLocked(companyID, "expired")
		ok = false
	}
	var checkedAt time.Time
	var cachedVersion string
	if ok {
		checkedAt, cachedVersion = cached.checkedAt, cached.version
	}
	r.mu.Unlock()

	if ok {
		if now.Sub(checkedAt) < r.config.VersionCheck {
			return r.copyIfCurrent(ctx, companyID, cached)
		}
		version, err := r.secrets.CurrentVersion(ctx, r.SecretName(companyID))
		if errors.Is(err, ErrNoKey) {
			r.Invalidate(companyID)
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("byok: check key version: %w", err)
		}
		if version == cachedVersion {
			r.mu.Lock()
			cached.checkedAt = now
			r.mu.Unlock()
			return r.copyIfCurrent(ctx, companyID, cached)
		}
		r.mu.Lock()
		if r.entries[companyID] == cached {
			r.evictLocked(companyID, "rotated")
		}
		r.mu.Unlock()
	}
	return r.load(ctx, companyID)
}

// copyIfCurrent clones the cached key, or reloads if it was evicted after
// the caller looked it up.
func (r *Resolver) copyIfCurrent(ctx context.Context, companyID string, cached *entry) (*aigateway.Key, error) {
	r.mu.Lock()
	var key *aigateway.Key
	if r.entries[companyID] == cached {
		key = cached.key.Clone()
	}
	r.mu.Unlock()
	if key != nil {
		return key, nil
	}
	return r.load(ctx, companyID)
}

func (r *Resolver) load(ctx context.Context, companyID string) (*aigateway.Key, error) {
	r.mu.Lock()
	// Another caller may have finished loading since this one missed.
	if cached, ok := r.entries[companyID]; ok && r.now().Before(cached.expires) {
		key := cached.key.Clone()
		r.mu.Unlock()
		return key, nil
	}
	if pending, ok := r.loads[companyID]; ok {
		r.mu.Unlock()
		select {
		case <-pending.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if pending.err != nil {
			return nil, pending.err
		}
		r.mu.Lock()
		key := pending.key.Clone()
		r.mu.Unlock()
		if key.Zeroed() {
			// Evicted between the load and this read: load again.
			return r.load(ctx, companyID)
		}
		return key, nil
	}
	pending := &load{done: make(chan struct{})}
	r.loads[companyID] = pending
	r.mu.Unlock()

	pending.key, pending.version, pending.err = r.fetch(ctx, companyID)
	r.mu.Lock()
	delete(r.loads, companyID)
	if pending.err == nil {
		if old, ok := r.entries[companyID]; ok {
			old.key.Zero()
		}
		now := r.now()
		r.entries[companyID] = &entry{key: pending.key, version: pending.version, expires: now.Add(r.config.TTL), checkedAt: now}
	}
	var key *aigateway.Key
	if pending.err == nil {
		key = pending.key.Clone()
	}
	r.mu.Unlock()
	close(pending.done)
	return key, pending.err
}

func (r *Resolver) fetch(ctx context.Context, companyID string) (*aigateway.Key, string, error) {
	secret, err := r.secrets.Get(ctx, r.SecretName(companyID))
	if err != nil {
		if errors.Is(err, ErrNoKey) {
			return nil, "", err
		}
		return nil, "", fmt.Errorf("byok: read secret: %w", err)
	}
	plaintext, err := open(ctx, r.kms, companyID, secret.Value)
	if err != nil {
		return nil, "", err
	}
	defer zero(plaintext)
	r.config.Logger.Info("byok key loaded", slog.String("company_id", companyID), slog.String("version", secret.VersionID))
	return aigateway.NewKey(plaintext), secret.VersionID, nil
}

// Invalidate drops and zeroes companyID's cached key, for a rotation
// notification or an operator action.
func (r *Resolver) Invalidate(companyID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictLocked(companyID, "invalidated")
}

// Sweep evicts every expired entry. Run calls it periodically so an idle
// company's key does not stay in memory past its TTL.
func (r *Resolver) Sweep() int {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	evicted := 0
	for companyID, cached := range r.entries {
		if !now.Before(cached.expires) {
			r.evictLocked(companyID, "expired")
			evicted++
		}
	}
	return evicted
}

// Run sweeps until ctx is done, then zeroes every cached key.
func (r *Resolver) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.Close()
			return
		case <-ticker.C:
			r.Sweep()
		}
	}
}

// Close zeroes and drops every cached key.
func (r *Resolver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for companyID := range r.entries {
		r.evictLocked(companyID, "closed")
	}
}

func (r *Resolver) evictLocked(companyID, reason string) {
	cached, ok := r.entries[companyID]
	if !ok {
		return
	}
	cached.key.Zero()
	delete(r.entries, companyID)
	r.config.Logger.Info("byok key evicted", slog.String("company_id", companyID), slog.String("reason", reason))
}
