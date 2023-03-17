package confluentic

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/database"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type kafkaC struct {
	*kafka.Consumer
}

type consumerHandler struct {
	config *config.Configuration
	db     *database.DB
	kafkaC kafkaC
}

func NewConsumerHandler(cf *config.Configuration, db *database.DB) (iface.ConsumerHandler, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		cf.KAFKASERVER:      cf.KAFKAURL,
		"group.id":          cf.KAFKAGROUPID,
		"auto.offset.reset": cf.KAFKARESETPOLICY,
	})

	if err != nil {
		return nil, err
	}

	return &consumerHandler{
		db:     db,
		config: cf,
		kafkaC: kafkaC{c},
	}, nil
}

func (h *consumerHandler) ReadMessagesFromKafka() error {
	return h.kafkaC.readMessagesFromKafka(h)
}

func (h *consumerHandler) Close() error {
	return h.kafkaC.Close()
}

func (h *consumerHandler) Subscribe() error {
	topics := []string{h.config.KAFKATOPIC}
	return h.kafkaC.subscribe(topics)
}

func (k *kafkaC) subscribe(topics []string) error {
	err := k.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}
	return nil
}

func (k *kafkaC) readMessagesFromKafka(h *consumerHandler) error {
	var err error
	fmt.Println("Start reading Kafka messages")
	for {
		msg, err := k.ReadMessage(-1)

		if err == nil {
			var job model.Job
			fmt.Printf("\nReceived msg from Kafka partition: %s\n", msg.TopicPartition)
			err = json.Unmarshal(msg.Value, &job)
			if err != nil {
				break
			}
			err = h.db.SaveJob(&job)
			if err != nil {
				break
			}
		} else {
			log.Printf("consumer error: %v (%v)\n", err, msg)
			break
		}
	}
	return err
}
