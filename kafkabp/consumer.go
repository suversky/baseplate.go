package kafkabp

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"

	"github.com/reddit/baseplate.go/log"
	"github.com/reddit/baseplate.go/metricsbp"
	"github.com/reddit/baseplate.go/tracing"
)

// ConsumeMessageFunc is a function type for consuming consumer messages.
type ConsumeMessageFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

// ConsumeErrorFunc is a function type for consuming consumer errors.
type ConsumeErrorFunc func(err error)

// consumer is an instance of a Kafka consumer.
type consumer struct {
	cfg     ConsumerConfig
	topic   string
	offset  int64
	tracing bool
	name    string

	consumer           atomic.Value // sarama.Consumer
	partitions         atomic.Value // []int32
	partitionConsumers atomic.Value // []sarama.PartitionConsumer

	closed          int64
	consumeReturned int64

	wg sync.WaitGroup
}

// Consumer defines the interface of a consumer struct.
type Consumer interface {
	io.Closer

	Consume(ConsumeMessageFunc, ConsumeErrorFunc) error

	// IsHealthy returns false after Consume returns.
	IsHealthy() bool
}

func (kc *consumer) getConsumer() sarama.Consumer {
	c, _ := kc.consumer.Load().(sarama.Consumer)
	return c
}

func (kc *consumer) getPartitions() []int32 {
	p, _ := kc.partitions.Load().([]int32)
	return p
}

func (kc *consumer) getPartitionConsumers() []sarama.PartitionConsumer {
	pc, _ := kc.partitionConsumers.Load().([]sarama.PartitionConsumer)
	return pc
}

// Reset recreates the consumer and assigns partitions.
func (kc *consumer) Reset() error {
	if c := kc.getConsumer(); c != nil {
		if err := c.Close(); err != nil {
			log.Warnw("Error closing the consumer", "err", err)
		}
	}

	rebalance := func() (err error) {
		c, err := NewSaramaConsumer(kc.cfg.Brokers, kc.cfg.SaramaConfig)
		if err != nil {
			return err
		}

		partitions, err := c.Partitions(kc.cfg.Topic)
		if err != nil {
			return err
		}

		kc.consumer.Store(c)
		kc.partitions.Store(partitions)
		return nil
	}

	err := rebalance()
	if err != nil {
		metricsbp.M.Counter("kafka.consumer.rebalance.failure").Add(1)
		return err
	}

	metricsbp.M.Counter("kafka.consumer.rebalance.success").Add(1)
	return nil
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	// Validate input parameters.
	if cfg.SaramaConfig == nil {
		cfg.SaramaConfig = DefaultSaramaConfig()
	}
	if cfg.ClientID != "" {
		cfg.SaramaConfig.ClientID = cfg.ClientID
	}
	if cfg.SaramaConfig.ClientID == "" {
		return nil, ErrClientIDEmpty
	}

	if len(cfg.Brokers) == 0 {
		return nil, ErrBrokersEmpty
	}

	if cfg.Topic == "" {
		return nil, ErrTopicEmpty
	}

	if cfg.Offset != OffsetNewest {
		cfg.Offset = OffsetOldest
	}

	if cfg.Tracing == nil {
		*cfg.Tracing = true
	}

	// Return any errors that occurred while consuming on the Errors channel.
	cfg.SaramaConfig.Consumer.Return.Errors = true

	kc := &consumer{
		cfg:     cfg,
		topic:   cfg.Topic,
		offset:  int64(cfg.Offset),
		tracing: *cfg.Tracing,
	}

	// Initialize Sarama consumer and set atomic values.
	if err := kc.Reset(); err != nil {
		return nil, err
	}

	return kc, nil
}

// Close closes all partition consumers first, then the parent consumer.
func (kc *consumer) Close() error {
	// Return early if closing is already in progress
	if !atomic.CompareAndSwapInt64(&kc.closed, 0, 1) {
		return nil
	}

	partitionConsumers := kc.getPartitionConsumers()
	for _, pc := range partitionConsumers {
		// leaves room to drain pc's message and error channels
		pc.AsyncClose()
	}
	// wait for the Consume function to return
	kc.wg.Wait()
	return kc.getConsumer().Close()
}

// Consume consumes Kafka messages and errors from each partition's consumer.
// It is necessary to call Close() on the KafkaConsumer instance once all
// operations are done with the consumer instance.
func (kc *consumer) Consume(
	messagesFunc ConsumeMessageFunc,
	errorsFunc ConsumeErrorFunc,
) error {
	defer atomic.StoreInt64(&kc.consumeReturned, 1)
	kc.wg.Add(1)
	defer kc.wg.Done()

	// sarama could close the channels (and cause the goroutines to finish) in
	// two cases, where we want different behavior:
	//   - in case of partition rebalance: restart goroutines
	//   - in case of call to Close/AsyncClose: exit
	var wg sync.WaitGroup
	for {
		// create a partition consumer for each partition
		consumer := kc.getConsumer()
		partitions := kc.getPartitions()
		partitionConsumers := make([]sarama.PartitionConsumer, 0, len(partitions))

		for _, p := range partitions {
			partitionConsumer, err := consumer.ConsumePartition(kc.topic, p, kc.offset)
			if err != nil {
				return err
			}
			partitionConsumers = append(partitionConsumers, partitionConsumer) // for closing individual partitions when Close() is called

			// consume partition consumer messages
			wg.Add(1)
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				for m := range pc.Messages() {
					// Wrap in anonymous function for easier defer.
					func() {
						ctx := context.Background()
						var err error
						if kc.tracing {
							var span *tracing.Span
							spanName := "consumer." + kc.topic
							ctx, span = tracing.StartTopLevelServerSpan(ctx, spanName)
							defer func() {
								span.FinishWithOptions(tracing.FinishOptions{
									Ctx: ctx,
									Err: err,
								}.Convert())
							}()
						}

						err = messagesFunc(ctx, m)
					}()
				}
			}(partitionConsumer)

			wg.Add(1)
			// consume partition consumer errors
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				for err := range pc.Errors() {
					errorsFunc(err)
				}
			}(partitionConsumer)
		}
		kc.partitionConsumers.Store(partitionConsumers)

		wg.Wait()

		if atomic.LoadInt64(&kc.closed) != 0 {
			return nil
		}

		if err := kc.Reset(); err != nil {
			return err
		}
	}
}

// IsHealthy returns true until Consume returns, then false thereafter.
func (kc *consumer) IsHealthy() bool {
	return atomic.LoadInt64(&kc.consumeReturned) == 0
}
