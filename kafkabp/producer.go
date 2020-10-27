package kafkabp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/reddit/baseplate.go/log"
	"github.com/reddit/baseplate.go/metricsbp"
)

// Producer defines an interface for how producers should publish messages to
// Kafka.
type Producer interface {
	Publish(ctx context.Context, msg ProducerMessage)
	Close() error
}

// TopicAsyncProducer is used to send messages to a given Kafka topic
// asynchronously.
type TopicAsyncProducer struct {
	Producer sarama.AsyncProducer
	cfg      ProducerConfig
	sc       *sarama.Config

	closed int64
}

// ProducerMessage defines the message that should be sent to Publish.
type ProducerMessage struct {
	// Required. Topic is the topic to publish to.
	Topic string

	// Required. Data is the actual message to store in Kafka.
	Data []byte

	// Optional. Key specifies a partitioning key for Data if supplied.
	Key []byte

	// Optional. Timestamp might provide the timestamp for a message, but this
	// depends on broker configuration. Please consult the following
	// documentation for more details:
	// https://godoc.org/github.com/Shopify/sarama#ProducerMessage
	Timestamp time.Time
}

// InitTopicAsyncProducer initializes a TopicAsyncProducer from the provided
// configuration.
func InitTopicAsyncProducer(cfg ProducerConfig) (Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrBrokersEmpty
	}

	sc, err := cfg.NewSaramaConfig()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}

	cfg.Logger = log.ErrorWithSentryWrapper()
	go func() {
		counter := metricsbp.M.Counter("kafka.producer.errors")
		for err := range producer.Errors() {
			cfg.Logger.Log(context.Background(), err.Error())
			counter.Add(1)
		}
	}()

	return &TopicAsyncProducer{
		Producer: producer,
		cfg:      cfg,
		sc:       sc,
	}, nil
}

// Publish publishes msg to the Kafka topic specified in msg.Topic.
func (tp *TopicAsyncProducer) Publish(ctx context.Context, msg ProducerMessage) {
	if atomic.LoadInt64(&tp.closed) != 0 {
		str := "kafkabp.TopicAsyncProducer: Publish called after Close is called"
		tp.cfg.Logger.Log(context.Background(), str)
		return
	}

	message := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Value:     sarama.ByteEncoder(msg.Data),
		Timestamp: msg.Timestamp,
	}

	if len(msg.Key) > 0 {
		message.Key = sarama.ByteEncoder(msg.Key)
	}

	tp.Producer.Input() <- message
}

// Close stops the producer from publishing and blocks until all messages are
// published (or have errored out).
func (tp *TopicAsyncProducer) Close() error {
	atomic.StoreInt64(&tp.closed, 1)
	return tp.Producer.Close()
}
