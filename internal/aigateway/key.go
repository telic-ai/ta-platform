package aigateway

import (
	"log/slog"
	"sync"
)

const redacted = "[REDACTED]"

// Key holds a provider API key in memory. It never renders its value
// through fmt, slog, or encoding/json, and Zero overwrites the bytes so an
// evicted key does not linger in the heap.
type Key struct {
	mu    sync.RWMutex
	value []byte
}

// NewKey copies secret into a new Key. Callers should zero their own copy.
func NewKey(secret []byte) *Key {
	value := make([]byte, len(secret))
	copy(value, secret)
	return &Key{value: value}
}

// Reveal returns the key as a string for the one call that must send it
// (the provider request header). The result must not be stored or logged.
func (k *Key) Reveal() string {
	if k == nil {
		return ""
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	return string(k.value)
}

// Clone returns an independent copy, so the original can be zeroed
// without affecting it. Cloning a zeroed key yields a zeroed key.
func (k *Key) Clone() *Key {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return NewKey(k.value)
}

// Zeroed reports whether Zero has been called (or the key is empty).
func (k *Key) Zeroed() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.value) == 0
}

// Zero overwrites the key material and releases it.
func (k *Key) Zero() {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for i := range k.value {
		k.value[i] = 0
	}
	k.value = nil
}

func (k *Key) String() string               { return redacted }
func (k *Key) GoString() string             { return redacted }
func (k *Key) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (k *Key) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (k *Key) MarshalText() ([]byte, error) { return []byte(redacted), nil }
