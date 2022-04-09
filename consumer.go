// Package skyffel_kafka is a wrapper for confluent-kafka-go with cross-cluster at-least-once capabilities
package skyffel_kafka

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	defcon "github.com/kjansson/defcon"
)

type Consumer struct {
	config         Config
	Consumer       *kafka.Consumer
	MessageChannel chan Message
	CommitChannel  chan MessageInfo
}

func (k *Consumer) Commit(m MessageInfo) error {
	_, err := k.Consumer.CommitOffsets([]kafka.TopicPartition{{
		Topic:     &m.Topic,
		Partition: int32(m.Partition),
		Offset:    kafka.Offset(m.Offset),
	}})
	return err
}

func NewConsumer(config Config) *Consumer {

	logger := log.New(os.Stderr, fmt.Sprintf("%s:\t", "skyffel-kafka-consumer"), log.Ldate|log.Ltime|log.Lshortfile)

	var assignedPartitions map[string][]int

	err := defcon.CheckConfigStruct(&config)
	if err != nil {
		logger.Fatal("Error while parsing config:", err)
	}
	kafkaConfig := kafka.ConfigMap{}
	// Transfer kafka config to confluent-kafka-go
	for key, val := range config.KafkaConfig {
		kafkaConfig[key] = val
	}

	consumer, e := kafka.NewConsumer(&kafkaConfig)
	if e != nil {
		logger.Fatal("Kafka consumer creation error: " + e.Error())
	}

	err = consumer.SubscribeTopics(config.KafkaTopics, nil)
	if err != nil {
		logger.Fatalln("Could not subscribe to topics: ", err)
	}

	dataChannel := make(chan Message)
	commitChannel := make(chan MessageInfo)

	c := &Consumer{
		Consumer:       consumer,
		MessageChannel: dataChannel,
		CommitChannel:  commitChannel,
		config:         config,
	}

	go func() {
		for {
			commit := <-commitChannel
			c.Commit(commit)
			if config.Debug {
				logger.Println("DEBUG: consumer commit.")
			}
		}
	}()

	go func() {

		defer consumer.Close()

		for {

			ev := consumer.Poll(500)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {

			case *kafka.Message:
				upstreamOffset, _ := strconv.Atoi(e.TopicPartition.Offset.String())
				outgoingData := Message{
					Data:      e.Value,
					Partition: int(e.TopicPartition.Partition),
					Topic:     *e.TopicPartition.Topic,
					Offset:    upstreamOffset,
					Key:       string(e.Key),
				}
				dataChannel <- outgoingData
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					logger.Println("No brokers present!")
				}
			case kafka.AssignedPartitions:
				consumer.Assign(e.Partitions)
				topicPartitionAssignments := assignedPartitions[*e.Partitions[0].Topic]
				for _, p := range e.Partitions {
					topicPartitionAssignments = append(topicPartitionAssignments, int(p.Partition))
				}
				assignedPartitions[*e.Partitions[0].Topic] = topicPartitionAssignments

			case kafka.RevokedPartitions:
				consumer.Unassign()
			}
		}
	}()
	return c
}
