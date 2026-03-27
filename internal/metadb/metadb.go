package metadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Client wraps a connection to the Airflow metadata database.
type Client struct {
	db *sql.DB
}

// New opens a connection to the metadata database.
func New(dsn string) (*Client, error) {
	driver := "pgx"
	if strings.HasPrefix(dsn, "mysql://") || strings.Contains(dsn, "@tcp(") {
		return nil, fmt.Errorf("mysql metadata DB not yet supported, use postgres")
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open metadata DB: %w", err)
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping metadata DB: %w", err)
	}

	return &Client{db: db}, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}

// SLAMissCount returns the total number of SLA misses.
func (c *Client) SLAMissCount(ctx context.Context) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sla_miss").Scan(&count)
	return count, err
}

// XComStats returns row count and estimated total byte size of XCom values.
func (c *Client) XComStats(ctx context.Context) (rows int64, bytes int64, err error) {
	err = c.db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM(octet_length(value::text)), 0) FROM xcom",
	).Scan(&rows, &bytes)
	return
}

// ExecutorSlots returns task instance counts by state for executor capacity tracking.
func (c *Client) ExecutorSlots(ctx context.Context) (map[string]int64, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT COALESCE(state, 'no_status'), COUNT(*)
		 FROM task_instance
		 WHERE state IN ('running', 'queued', 'scheduled')
		 GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]int64)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		result[state] = count
	}
	return result, rows.Err()
}
