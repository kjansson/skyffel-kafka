// Package skyffel_kafka is a wrapper for confluent-kafka-go with cross-cluster at-least-once capabilities
package skyffel_kafka

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	defcon "github.com/kjansson/defcon"
)

// Producer is a wrapper struct for the underlying confluent-kafka producer, along with channels for producing messages, reading events, and offset information
type Producer struct {
	Producer          *kafka.Producer  // Underlying confluent-kafka-go producer
	MessageChannel    chan Message     // Channel for outgoing messages
	EventChannel      chan kafka.Event // Channel for event forwarding (if enabled)
	OffsetInfoChannel chan MessageInfo // Channel for offset information. Offsets received on this channel (if CommitManually is enabled) are safe to commit
}

type internalTopicMetadata struct {
	lastReportedOffset map[int]int64
	partitions         int
}

// NewProducer returns a wrapped confluent-kafka producer, witch channels for sending messages, reading events, and offset information
func NewProducer(config Config) *Producer {

	logger := log.New(os.Stderr, fmt.Sprintf("%s:\t", "skyffel-kafka-producer"), log.Ldate|log.Ltime|log.Lshortfile)

	var trackingLock = sync.RWMutex{}
	var message Message
	var run bool = true
	upstreamTopicInternalTracking := make(map[string]internalTopicMetadata)
	var partition int32

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)

	offsetChannel := make(chan MessageInfo, config.CommitChannelSize)
	eventChannel := make(chan kafka.Event, config.EventChannelSize)
	MessageChannel := make(chan Message, config.MessageChannelSize)

	err := defcon.CheckConfigStruct(&config)
	if err != nil {
		logger.Fatal("Error while parsing config:", err)
	}
	kafkaConfig := kafka.ConfigMap{}
	// Transfer kafka config to confluent Kafka
	for key, val := range config.KafkaConfig {
		kafkaConfig[key] = val
	}

	if config.CommitManually {
		kafkaConfig["enable.idempotence"] = true
		kafkaConfig["request.required.acks"] = "all"
	}

	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		logger.Println("Could not create producer, ", err)
	}

	p := &Producer{
		Producer:          producer,
		MessageChannel:    MessageChannel,
		EventChannel:      eventChannel,
		OffsetInfoChannel: offsetChannel,
	}

	internalDeliveryReportChannel := make(chan MessageInfo, 1000)

	type offsetLink struct {
		offset    int
		delivered bool
		next      *offsetLink
		previous  *offsetLink
	}

	offsetLinkMap := make(map[string]map[int]offsetLink)

	go func() {

		for {
			report := <-internalDeliveryReportChannel

			if _, ok := offsetLinkMap[report.Topic][report.Partition]; !ok {
				offsetLinkMap[report.Topic][report.Partition] = offsetLink{
					offset:    int(report.Offset),
					next:      nil,
					delivered: true,
				}
			} else {

				current := offsetLinkMap[report.Topic][report.Partition]

				for {
					if current.offset == int(report.Offset) {
						break
					}
					if current.delivered {
						trackingLock.Lock()
						upstreamTopicInternalTracking[report.Topic].lastReportedOffset[report.Partition] = int64(current.offset)
						trackingLock.Unlock()
					}
					if current.next == nil && current.offset != int(report.Offset)-1 {
						current.next = &offsetLink{
							offset:    current.offset + 1,
							next:      nil,
							delivered: false,
						}
					}
					if current.next == nil && current.offset == int(report.Offset)-1 {
						current.next = &offsetLink{
							offset:    int(report.Offset),
							next:      nil,
							delivered: true,
						}
					}
					current = *current.next
				}
			}
		}

	}()

	if config.CommitManually {
		go func() {
			for {
				time.Sleep(time.Duration(config.CommitIntervalMs) * time.Millisecond)

				trackingLock.RLock()

				for topic, topicMetadata := range upstreamTopicInternalTracking { // Range through topics in the internal tracking

					for partition, lastReported := range topicMetadata.lastReportedOffset { // Range through partitions

						commitMessage := MessageInfo{ // Create a commitMessage for the latest reported offset
							Topic:     topic,
							Partition: partition,
							Offset:    lastReported,
						}
						if config.Consumer == nil { // Send to offset channel
							select {
							case offsetChannel <- commitMessage:
							default:
								logger.Fatalln("Offset channel is full, are offsets not being handled? Exiting.")
							}
						} else { // If we have the consumer pointer, send directly to the consumers commit channel
							if config.Debug {
								logger.Println("Sending commit information directly to consumer.")
							}
							config.Consumer.CommitChannel <- commitMessage
							if config.Debug {
								logger.Println("Commit sent.")
							}
						}
					}
				}
				trackingLock.RUnlock()
			}
		}()

		go func() {
			for e := range producer.Events() {
				if config.ForwardEvents {
					eventChannel <- e
				}
				switch ev := e.(type) {
				case *kafka.Error:
					logger.Fatalln("Kafka error:", err)
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fatalErr := producer.GetFatalError()
						if fatalErr != nil {
							logger.Fatalln("Producer fatal error.")
						}
					} else {
						if ev.TopicPartition.Error == nil {
							messageMetadata := ev.Opaque.(MessageInfo)
							trackingLock.Lock()
							upstreamTopicInternalTracking[messageMetadata.Topic].lastReportedOffset[messageMetadata.Partition] = messageMetadata.Offset
							trackingLock.Unlock()
						}
					}
				}
			}
		}()
	} else {
		go func() {
			for e := range producer.Events() {
				if config.ForwardEvents {
					eventChannel <- e
				}
			}
		}()
	}
	go func() {
		for {
			if run {
				select {
				case <-signalChannel:
					logger.Println("Caught SIGTERM. Beginning shutdown.")
					run = false
				case message = <-MessageChannel:

					if _, ok := upstreamTopicInternalTracking[message.Topic]; !ok {

						topicTracking := internalTopicMetadata{}
						topicMetadata, err := producer.GetMetadata(&message.Topic, false, 5000)
						topicTracking.partitions = len(topicMetadata.Topics[message.Topic].Partitions)
						fmt.Printf("Registered new topic %s with %d partitions", message.Topic, topicTracking.partitions)
						if err != nil {
							fmt.Printf("Could not get metadata from broker: %s\n", err)
							return
						}
						topicTracking.lastReportedOffset = make(map[int]int64)
						for i := 0; i < int(topicTracking.partitions); i++ {
							topicTracking.lastReportedOffset[i] = 0
						}
						upstreamTopicInternalTracking[message.Topic] = topicTracking
					}

					msgMetaData := MessageInfo{
						Partition: message.Partition,
						Topic:     message.Topic,
						Offset:    int64(message.Offset),
					}

					if config.CustomPartitioner == nil {
						partition, err = murmur2Hash(message.Key, int32(upstreamTopicInternalTracking[message.Topic].partitions))
					} else {
						partition, err = config.CustomPartitioner(message.Key, int32(upstreamTopicInternalTracking[message.Topic].partitions))
					}
					if err == nil {
						partition = int32(rand.Intn(upstreamTopicInternalTracking[message.Topic].partitions))
						if config.CustomPartitioner == nil {
							partition, err = murmur2Hash(message.Key, int32(upstreamTopicInternalTracking[message.Topic].partitions))
						} else {
							partition, err = config.CustomPartitioner(message.Key, int32(upstreamTopicInternalTracking[message.Topic].partitions))
						}
						if err != nil {
							logger.Println("Warning: partitioner could not use message key, using random partition:", err)
							partition = int32(rand.Intn(upstreamTopicInternalTracking[message.Topic].partitions))
						}
						producer.ProduceChannel() <- &kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &message.Topic, Partition: partition},
							Value:          []byte(message.Data),
							Opaque:         msgMetaData,
						}
						if config.Debug {
							logger.Println("DEBUG: message produced on topic", message.Topic)
						}
					} else {
						logger.Println("Error partitioning for message: ", err)
					}
				}
			} else {
				logger.Println("SIGTERM caught. Flushing buffers before exiting.")
				remaining := producer.Flush(config.FlushOnExitTimeout)
				if remaining != 0 {
					logger.Println("WARNING: exiting with unflushed messages!")
				}
				logger.Println("Flushed everything, exiting.")
			}
		}
	}()
	return p
}
