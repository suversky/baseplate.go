package kafkabp

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/reddit/baseplate.go/log"
)

// ConsumerConfig can be used to configure a kafkabp Consumer.
//
// Can be deserialized from YAML.
//
// Example:
//
// kafkaConsumer:
//   brokers:
//     - 127.0.0.1:9090
//     - 127.0.0.2:9090
//   topic: sample-topic
//   clientID: myclient
//   offset: oldest
type ConsumerConfig struct {
	// Required. Brokers specifies a slice of broker addresses.
	Brokers []string `yaml:"brokers"`

	// Required. Topic is used to specify the topic to consume.
	Topic string `yaml:"topic"`

	// Required. ClientID is a user-provided string sent with every request to
	// the brokers for logging, debugging, and auditing purposes. The default
	// Consumer implementation in this library expects every Consumer to have a
	// unique ClientID.
	//
	// The Kubernetes pod ID is usually a good candidate for this unique ID.
	ClientID string `yaml:"clientID"`

	// Optional. Defaults to "oldest". Valid values are "oldest" and "newest".
	Offset string `yaml:"offset"`
}

// NewSaramaConfig instantiates a sarama.Config with sane consumer defaults
// from sarama.NewConfig(), overwritten by values parsed from cfg.
func (cfg *ConsumerConfig) NewSaramaConfig() (*sarama.Config, error) {
	c := sarama.NewConfig()

	// Return any errors that occurred while consuming on the Errors channel.
	c.Consumer.Return.Errors = true

	// Validate input parameters.
	if len(cfg.Brokers) == 0 {
		return nil, ErrBrokersEmpty
	}

	if cfg.Topic == "" {
		return nil, ErrTopicEmpty
	}

	if cfg.ClientID == "" {
		return nil, ErrClientIDEmpty
	}

	switch cfg.Offset {
	case "":
		// OffsetOldest is the "true" default case (in that it will be reached if
		// an offset isn't specified).
		fallthrough
	case "oldest":
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		c.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return nil, ErrOffsetInvalid
	}

	return c, nil
}

// ProducerConfig can be used to configure a kafkabp Producer.
//
// Can be deserialized from YAML.
//
// Example:
//
// kafkaProducer:
//   brokers:
//     - 127.0.0.1:9090
//     - 127.0.0.2:9090
type ProducerConfig struct {
	// Required. Brokers specifies a slice of broker addresses.
	Brokers []string `yaml:"brokers"`

	// Optional. When non-nil, it will be used to log errors.
	Logger log.Wrapper
}

// NewSaramaConfig instantiates a sarama.Config with sane producer defaults
// from sarama.NewConfig(), overwritten by values parsed from cfg.
func (cfg *ProducerConfig) NewSaramaConfig() (*sarama.Config, error) {
	c := sarama.NewConfig()

	// Only wait for the leader to ack a Publish call.
	c.Producer.RequiredAcks = sarama.WaitForLocal

	// Leave messages uncompressed.
	c.Producer.Compression = sarama.CompressionNone

	// Flush batches every 100ms.
	c.Producer.Flush.Frequency = 100 * time.Millisecond

	return c, nil
}
