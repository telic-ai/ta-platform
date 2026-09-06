// Package migrations owns the ordered Postgres schema migrations.
package migrations

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

const migrationLockID int64 = 0x5441504c415446 // "TAPL ATF", stable for this application.

type migration struct {
	version int64
	name    string
	up      string
	down    string
}

// Runner applies embedded migrations to a pool.
type Runner struct {
	pool *pgxpool.Pool
}

// New returns a migration runner backed by pool.
func New(pool *pgxpool.Pool) *Runner { return &Runner{pool: pool} }

// Up applies every unapplied migration in version order.
func (r *Runner) Up(ctx context.Context) error {
	migrations, err := load()
	if err != nil {
		return err
	}
	return r.withLock(ctx, func(conn *pgxpool.Conn) error {
		if err := ensureVersionTable(ctx, conn); err != nil {
			return err
		}
		for _, m := range migrations {
			var applied bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, m.version).Scan(&applied); err != nil {
				return fmt.Errorf("migration %d: check version: %w", m.version, err)
			}
			if applied {
				continue
			}
			if err := apply(ctx, conn, m, true); err != nil {
				return err
			}
		}
		return nil
	})
}

// Down rolls back the most recently applied migration. It is a no-op when
// the database is already at version zero.
func (r *Runner) Down(ctx context.Context) error {
	migrations, err := load()
	if err != nil {
		return err
	}
	byVersion := make(map[int64]migration, len(migrations))
	for _, m := range migrations {
		byVersion[m.version] = m
	}
	return r.withLock(ctx, func(conn *pgxpool.Conn) error {
		if err := ensureVersionTable(ctx, conn); err != nil {
			return err
		}
		var version int64
		err := conn.QueryRow(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("find current migration: %w", err)
		}
		m, ok := byVersion[version]
		if !ok {
			return fmt.Errorf("database has unknown migration version %d", version)
		}
		return apply(ctx, conn, m, false)
	})
}

func (r *Runner) withLock(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) }()
	return fn(conn)
}

func ensureVersionTable(ctx context.Context, conn *pgxpool.Conn) error {
	_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
        version bigint PRIMARY KEY,
        name text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
    )`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

func apply(ctx context.Context, conn *pgxpool.Conn, m migration, up bool) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migration %d: begin: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statement := m.up
	direction := "up"
	if !up {
		statement = m.down
		direction = "down"
	}
	if _, err := tx.Exec(ctx, statement); err != nil {
		return fmt.Errorf("migration %d %s: %w", m.version, direction, err)
	}
	if up {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.version)
	}
	if err != nil {
		return fmt.Errorf("migration %d %s: record version: %w", m.version, direction, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migration %d %s: commit: %w", m.version, direction, err)
	}
	return nil
}

func load() ([]migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	byVersion := map[int64]*migration{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		parts := strings.Split(entry.Name(), ".")
		if len(parts) != 3 || (parts[1] != "up" && parts[1] != "down") {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		prefix := strings.SplitN(parts[0], "_", 2)
		if len(prefix) != 2 {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version, err := strconv.ParseInt(prefix[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid migration filename %q: %w", entry.Name(), err)
		}
		body, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		m := byVersion[version]
		if m == nil {
			m = &migration{version: version, name: prefix[1]}
			byVersion[version] = m
		} else if m.name != prefix[1] {
			return nil, fmt.Errorf("migration version %d has conflicting names", version)
		}
		if parts[1] == "up" {
			m.up = string(body)
		} else {
			m.down = string(body)
		}
	}
	result := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.up == "" || m.down == "" {
			return nil, fmt.Errorf("migration %d must have both up and down files", m.version)
		}
		result = append(result, *m)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	return result, nil
}
