// Package clickhouse provides the shared ClickHouse client for services.
package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client wraps a ClickHouse connection.
type Client struct {
	conn driver.Conn
}

// New connects to ClickHouse using dsn, e.g.
// "clickhouse://localhost:9000/default" (see internal/platform/config).
func New(dsn string) (*Client, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}

	return &Client{conn: conn}, nil
}

// Conn returns the underlying driver connection for direct query access.
func (c *Client) Conn() driver.Conn {
	return c.conn
}

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Close releases the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
