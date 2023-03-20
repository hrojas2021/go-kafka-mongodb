package segmentio

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/database"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
	"github.com/segmentio/kafka-go"
)

type kafkaC struct {
	*kafka.Reader
}

type consumerHandler struct {
	config *config.Configuration
	db     *database.DB
	kafkaC kafkaC
}

func NewConsumerHandler(cf *config.Configuration, db *database.DB) (iface.ConsumerHandler, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cf.KAFKAURL},
		Topic:   cf.KAFKATOPIC,
	})

	return &consumerHandler{
		db:     db,
		config: cf,
		kafkaC: kafkaC{r},
	}, nil
}

func (h *consumerHandler) ReadMessagesFromKafka() error {
	return h.kafkaC.readMessagesFromKafka(h)
}

func (h *consumerHandler) Close() error {
	return h.kafkaC.Close()
}

func (h *consumerHandler) Subscribe() error {
	return nil
}

func (k *kafkaC) readMessagesFromKafka(h *consumerHandler) error {
	fmt.Println("Start reading Kafka messages in")
	var err error
	for {
		msg, err := k.ReadMessage(context.Background())
		if err != nil {
			break
		}

		fmt.Printf("\nReceived msg from Kafka partition with key %d: %s\n", msg.Partition, string(msg.Key))
		var job model.Job
		err = json.Unmarshal(msg.Value, &job)
		if err != nil {
			break
		}
		err = h.db.SaveJob(&job)
		if err != nil {
			break
		}
	}
	return err
}
