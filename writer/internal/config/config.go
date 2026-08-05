// Package config loads writer configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the writer.
type Config struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration

	KafkaBrokers   []string
	KafkaTopic     string
	KafkaGroup     string
	ConnectTimeout time.Duration

	ClickHouseAddr string
	ClickHouseDB   string
	ClickHouseUser string
	ClickHousePass string

	BatchMaxSize    int
	FlushInterval   time.Duration
	QueueCapacity   int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	LagReportPeriod time.Duration
}

// Load reads configuration from environment variables, applying defaults
// where a variable is unset.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        getEnv("WRITER_HTTP_ADDR", ":8080"),
		ShutdownTimeout: 10 * time.Second,

		KafkaBrokers:   splitCSV(getEnv("KAFKA_BROKERS", "redpanda:9092")),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "spans"),
		KafkaGroup:     getEnv("KAFKA_CONSUMER_GROUP", "writer"),
		ConnectTimeout: 10 * time.Second,

		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "clickhouse:9000"),
		ClickHouseDB:   getEnv("CLICKHOUSE_DB", "tracing"),
		ClickHouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass: getEnv("CLICKHOUSE_PASSWORD", ""),

		BatchMaxSize:    5000,
		FlushInterval:   2 * time.Second,
		QueueCapacity:   10000,
		InitialBackoff:  500 * time.Millisecond,
		MaxBackoff:      30 * time.Second,
		LagReportPeriod: 5 * time.Second,
	}

	var err error
	if cfg.BatchMaxSize, err = getIntEnv("WRITER_BATCH_MAX_SIZE", cfg.BatchMaxSize); err != nil {
		return Config{}, err
	}
	if cfg.QueueCapacity, err = getIntEnv("WRITER_QUEUE_CAPACITY", cfg.QueueCapacity); err != nil {
		return Config{}, err
	}
	if cfg.FlushInterval, err = getDurationEnv("WRITER_FLUSH_INTERVAL", cfg.FlushInterval); err != nil {
		return Config{}, err
	}
	if cfg.InitialBackoff, err = getDurationEnv("WRITER_INITIAL_BACKOFF", cfg.InitialBackoff); err != nil {
		return Config{}, err
	}
	if cfg.MaxBackoff, err = getDurationEnv("WRITER_MAX_BACKOFF", cfg.MaxBackoff); err != nil {
		return Config{}, err
	}
	if cfg.LagReportPeriod, err = getDurationEnv("WRITER_LAG_REPORT_PERIOD", cfg.LagReportPeriod); err != nil {
		return Config{}, err
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

func getIntEnv(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func getDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
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
