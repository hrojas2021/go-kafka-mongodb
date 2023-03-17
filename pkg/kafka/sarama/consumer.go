package sarama

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/database"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type kafkaC struct {
	sarama.ConsumerGroup
	*config.Configuration
}

type consumerHandler struct {
	db     *database.DB
	kafkaC kafkaC
}

type EventHandler interface {
	Handle(topic string, eventBytes []byte)
}

var ctx = context.Background()

func NewConsumerHandler(cf *config.Configuration, db *database.DB) (*consumerHandler, error) {
	servers := []string{cf.KAFKAURL}
	c, err := sarama.NewConsumerGroup(servers, cf.KAFKAGROUPID, nil)
	if err != nil {
		panic(err)
	}

	if err != nil {
		return nil, err
	}

	return &consumerHandler{
		db:     db,
		kafkaC: kafkaC{c, cf},
	}, nil
}

func (h *consumerHandler) ReadMessagesFromKafka() error {
	return h.kafkaC.readMessagesFromKafka(h)
}

func (h *consumerHandler) Close() error {
	return h.kafkaC.Close()
}

func (k *kafkaC) readMessagesFromKafka(h *consumerHandler) error {
	for {
		k.Consume(ctx, []string{k.KAFKATOPIC}, h)
	}
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("\nReceived from Kafka: %+v\n", string(msg.Value))
		var job model.Job
		fmt.Printf("\nReceived from Kafka partition: %d\n", msg.Partition)
		err := json.Unmarshal(msg.Value, &job)
		if err != nil {
			break
		}

		err = h.db.SaveJob(&job)
		if err != nil {
			break
		}
		session.MarkMessage(msg, "")
	}

	return nil
}
