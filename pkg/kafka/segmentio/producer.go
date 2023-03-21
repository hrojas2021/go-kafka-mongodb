package segmentio

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
	"github.com/segmentio/kafka-go"
)

type kafkaP struct {
	producer *kafka.Writer
	*config.Configuration
	counter int
}

func NewKafkaProducer(cf *config.Configuration) (iface.KafkaProducer, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{cf.KAFKAURL},
		Topic:   cf.KAFKATOPIC,
	})

	return &kafkaP{
		w, cf, 0,
	}, nil
}

func (k *kafkaP) SaveJobToKafka(job model.Job) error {
	d, err := json.Marshal(job)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("key-%d", k.counter)),
		Value: d,
	}

	err = k.producer.WriteMessages(context.Background(), msg)
	if err != nil {
		return err
	}
	k.counter++
	log.Printf("The job event has been created successfully \n")
	return nil
}

func (k *kafkaP) Close() error {
	log.Printf("Closing segmentio produder")
	return k.producer.Close()
}
