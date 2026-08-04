// Package config loads collector configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the collector.
type Config struct {
	GRPCAddr        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

// Load reads configuration from environment variables, applying defaults
// where a variable is unset.
func Load() (Config, error) {
	cfg := Config{
		GRPCAddr:        getEnv("COLLECTOR_GRPC_ADDR", ":4317"),
		HTTPAddr:        getEnv("COLLECTOR_HTTP_ADDR", ":4318"),
		ShutdownTimeout: 10 * time.Second,
	}

	if raw := os.Getenv("COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS: %w", err)
		}
		cfg.ShutdownTimeout = time.Duration(secs) * time.Second
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
