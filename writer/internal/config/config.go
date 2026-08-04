// Package config loads writer configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds all runtime configuration for the writer.
type Config struct {
	HTTPAddr        string
	KafkaBrokers    []string
	KafkaTopic      string
	ClickHouseAddr  string
	ClickHouseDB    string
	ClickHouseUser  string
	ClickHousePass  string
	ConnectTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// Load reads configuration from environment variables, applying defaults
// where a variable is unset.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        getEnv("WRITER_HTTP_ADDR", ":8080"),
		KafkaBrokers:    splitCSV(getEnv("KAFKA_BROKERS", "redpanda:9092")),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "otlp-spans"),
		ClickHouseAddr:  getEnv("CLICKHOUSE_ADDR", "clickhouse:9000"),
		ClickHouseDB:    getEnv("CLICKHOUSE_DB", "tracing"),
		ClickHouseUser:  getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass:  getEnv("CLICKHOUSE_PASSWORD", ""),
		ConnectTimeout:  10 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
