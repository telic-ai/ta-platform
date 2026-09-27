package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestWriterIsDurableAndKeyPartitioned(t *testing.T) {
	writer := New("broker:9092").Writer("session-events")
	defer writer.Close()

	if _, ok := writer.Balancer.(*kafkago.Hash); !ok {
		t.Errorf("Balancer = %T, want *kafka.Hash", writer.Balancer)
	}
	if writer.RequiredAcks != kafkago.RequireAll {
		t.Errorf("RequiredAcks = %d, want RequireAll", writer.RequiredAcks)
	}
	if writer.BatchTimeout != WriterBatchTimeout {
		t.Errorf("BatchTimeout = %v, want %v", writer.BatchTimeout, WriterBatchTimeout)
	}
}
