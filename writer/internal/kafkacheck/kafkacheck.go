// Package kafkacheck verifies connectivity to the Kafka-compatible broker
// (Redpanda) without consuming any messages.
package kafkacheck

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Ping connects to the given brokers and confirms reachability. It does not
// consume or produce any records; topic creation and consumption arrive in
// Phase 1.
func Ping(ctx context.Context, brokers []string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return fmt.Errorf("kafka client init: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}

	return nil
}
