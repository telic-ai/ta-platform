package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLocked means another holder has the lock.
var ErrLocked = errors.New("redis: lock is held")

// unlockScript deletes the key only if it still holds this holder's token,
// so a holder whose lock expired cannot release someone else's.
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

// Locker hands out expiring single-holder locks.
type Locker struct {
	rdb    *redis.Client
	prefix string
}

// NewLocker namespaces lock keys under prefix.
func NewLocker(client *Client, prefix string) *Locker {
	return &Locker{rdb: client.rdb, prefix: prefix}
}

// Acquire takes the lock for key for at most ttl. The returned function
// releases it; it is safe to call after the lock has expired.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("redis: lock token: %w", err)
	}
	token := hex.EncodeToString(raw)
	full := l.prefix + key
	ok, err := l.rdb.SetNX(ctx, full, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: acquire %s: %w", full, err)
	}
	if !ok {
		return nil, ErrLocked
	}
	return func(ctx context.Context) error {
		if err := unlockScript.Run(ctx, l.rdb, []string{full}, token).Err(); err != nil {
			return fmt.Errorf("redis: release %s: %w", full, err)
		}
		return nil
	}, nil
}
