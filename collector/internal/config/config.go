// Package config loads collector configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the collector.
type Config struct {
	GRPCAddr        string
	HTTPAddr        string
	ShutdownTimeout time.Duration

	KafkaBrokers         []string
	KafkaTopic           string
	KafkaMaxInFlight     int
	KafkaDeliveryTimeout time.Duration
}

// Load reads configuration from environment variables, applying defaults
// where a variable is unset.
func Load() (Config, error) {
	cfg := Config{
		GRPCAddr:        getEnv("COLLECTOR_GRPC_ADDR", ":4317"),
		HTTPAddr:        getEnv("COLLECTOR_HTTP_ADDR", ":4318"),
		ShutdownTimeout: 10 * time.Second,

		KafkaBrokers:         splitCSV(getEnv("KAFKA_BROKERS", "redpanda:9092")),
		KafkaTopic:           getEnv("KAFKA_TOPIC", "spans"),
		KafkaMaxInFlight:     2000,
		KafkaDeliveryTimeout: 30 * time.Second,
	}

	if raw := os.Getenv("COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS: %w", err)
		}
		cfg.ShutdownTimeout = time.Duration(secs) * time.Second
	}

	if raw := os.Getenv("KAFKA_MAX_IN_FLIGHT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid KAFKA_MAX_IN_FLIGHT: %w", err)
		}
		cfg.KafkaMaxInFlight = n
	}

	if raw := os.Getenv("KAFKA_DELIVERY_TIMEOUT_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid KAFKA_DELIVERY_TIMEOUT_SECONDS: %w", err)
		}
		cfg.KafkaDeliveryTimeout = time.Duration(secs) * time.Second
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
