// Package postgres provides the shared Postgres client for services.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pinger is the minimal interface store clients depend on, so callers can
// swap in a fake for unit tests.
type Pinger interface {
	Ping(ctx context.Context) error
	Close()
}

// Client wraps a pgx connection pool.
type Client struct {
	pool *pgxpool.Pool
}

var _ Pinger = (*Client)(nil)

// New connects to Postgres using dsn (see internal/platform/config).
func New(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	return &Client{pool: pool}, nil
}

// Pool returns the underlying pgx pool for callers that need direct query
// access.
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Close releases all pooled connections.
func (c *Client) Close() {
	c.pool.Close()
}
