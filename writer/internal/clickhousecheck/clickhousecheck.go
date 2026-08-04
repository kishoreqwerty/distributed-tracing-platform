// Package clickhousecheck verifies connectivity to ClickHouse without
// writing any data.
package clickhousecheck

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Options configures the ClickHouse connection used for the reachability
// check.
type Options struct {
	Addr     string
	Database string
	User     string
	Password string
}

// Ping opens a connection to ClickHouse and issues a round-trip ping. It
// does not create tables or write rows; schema application happens via
// deploy/clickhouse/init.sql at container startup.
func Ping(ctx context.Context, opts Options) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{opts.Addr},
		Auth: clickhouse.Auth{
			Database: opts.Database,
			Username: opts.User,
			Password: opts.Password,
		},
	})
	if err != nil {
		return fmt.Errorf("clickhouse client init: %w", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	return nil
}
