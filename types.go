// Package skyffel_kafka is a wrapper for confluent-kafka-go with cross-cluster at-least-once capabilities
package skyffel_kafka

type partitioner = func(partitionKey string, numPartitions int32) (int32, error)

type Config struct {
	KafkaConfig        map[string]string // Confluent-kafka-go configuration. All key-values here will be passed down to the underlying consumer/producer. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information on configuring.
	KafkaTopics        []string          // Consumer only. Topics to subscribe to.
	Debug              bool              // Enables debug output
	CommitManually     bool              // Enables commit information through channel for manual action. This also enables the idempotent producer. Defaults to false (automatic commits)
	CommitIntervalMs   int               `default:"100"` // Interval to commit offsets in milliseconds. Defaults to 100ms.
	ForwardEvents      bool              // Enables event forwarding through channel. Channel buffer size is determined by EventChannelSize, and will block when buffer is full. Defaults to false.
	FlushOnExitTimeout int               `default:"5"`     // Time to wait before exiting on non-fatal errors in seconds. During this time the buffers will try to flush. Default is 5.
	CommitChannelSize  int64             `default:"100"`   // Buffer size for the channel for commit information. If commits are handled manually, this channel must be read, otherwise it will block. Default is 100.
	MessageChannelSize int64             `default:"10000"` // Buffer size for outgoing messages
	EventChannelSize   int64             `default:"10000"` // Buffer size for event channel
	CustomPartitioner  partitioner       // If a custom partitioner is used, it can be defined here. If not defined, murmur2 will be used.
	Consumer           *Consumer         // Optional pointer to consumer, if this is not nil, commits will be handled automatically
}

// Message is the representation of a Kafka message used by skyffel_kafka
type Message struct {
	Data      []byte // Message data
	Topic     string // Incoming or outgoing topic
	Partition int    // Incoming or outgoing topic
	Offset    int    // Incoming offset
	Key       string // Key to partition on for outgoing message
}

// MessageInfo contains consumer-side metadata about a message, and is also used when CommitManually is enabled and will then be sent on the commit channel.
type MessageInfo struct {
	Offset    int64  // Offset of message.
	Topic     string // Originating topic
	Partition int    // Originating partition
}
