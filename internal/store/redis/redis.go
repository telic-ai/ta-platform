// Package redis provides the shared Redis client for services.
package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Client wraps a redis connection.
type Client struct {
	rdb *redis.Client
}

// New connects to Redis at addr (see internal/platform/config).
func New(addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	return &Client{rdb: rdb}, nil
}

// Raw returns the underlying go-redis client for direct command access.
func (c *Client) Raw() *redis.Client {
	return c.rdb
}

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping: %w", err)
	}
	return nil
}

// Close releases the connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
