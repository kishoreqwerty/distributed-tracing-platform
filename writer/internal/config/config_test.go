package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("CLICKHOUSE_ADDR", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "redpanda:9092" {
		t.Errorf("unexpected default KafkaBrokers: %v", cfg.KafkaBrokers)
	}
	if cfg.ClickHouseAddr != "clickhouse:9000" {
		t.Errorf("unexpected default ClickHouseAddr: %v", cfg.ClickHouseAddr)
	}
}

func TestLoadCustomBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092, broker2:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d: %v", len(cfg.KafkaBrokers), cfg.KafkaBrokers)
	}
}
