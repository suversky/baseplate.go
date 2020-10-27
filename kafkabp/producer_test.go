package kafkabp

import (
	"testing"
)

func TestInitAsyncProducer(t *testing.T) {
	// TestInitAsyncProducer with no brokers specified should not create a
	// producer and return ErrBrokersEmpty
	cfg := ProducerConfig{Brokers: []string{}}
	producer, err := InitTopicAsyncProducer(cfg)

	if producer != nil {
		t.Errorf("expected topicASyncProducer to be nil, got %v", producer)
	}

	if err != ErrBrokersEmpty {
		t.Errorf("expected err to be ErrBrokersEmpty, got %v", err)
	}

	// TODO: implement mock-based test of producer init here
	cfg.Brokers = []string{"127.0.0.1:9090", "127.0.0.2:9090"}
}

// TODO: implement mock-based test of producer publishing here
