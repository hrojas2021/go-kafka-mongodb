package confluentic

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/database"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type kafkaC struct {
	*kafka.Consumer
	*config.Configuration
}

type consumerHandler struct {
	db     *database.DB
	kafkaC kafkaC
}

func NewConsumerHandler(cf *config.Configuration, db *database.DB) (*consumerHandler, error) {
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
		kafkaC: kafkaC{c, cf},
	}, nil

}

func (h *consumerHandler) Subscribe() error {
	return h.kafkaC.subscribe()
}

func (h *consumerHandler) Close() error {
	return h.kafkaC.Close()
}

func (h *consumerHandler) ReadMessagesFromKafka() error {
	return h.kafkaC.readMessagesFromKafka(h.db)
}

func (k *kafkaC) subscribe() error {
	err := k.SubscribeTopics([]string{k.KAFKATOPIC}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (k *kafkaC) readMessagesFromKafka(db *database.DB) error {
	var err error
	fmt.Println("Start reading Kafka messages")
	for {
		msg, err := k.ReadMessage(-1)

		if err == nil {
			var job model.Job
			fmt.Printf("\nReceived from Kafka partition: %s\n", msg.TopicPartition)
			err = json.Unmarshal(msg.Value, &job)
			if err != nil {
				break
			}
			fmt.Printf("%+v\n", job)
			// err = db.SaveJob(&job)
			// if err != nil {
			// 	break
			// }
		} else {
			log.Printf("consumer error: %v (%v)\n", err, msg)
			break
		}
	}
	return err
}
