package confluentic

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type kafkaP struct {
	producer *kafka.Producer
	*config.Configuration
}

func NewKafkaProducer(cf *config.Configuration) (iface.KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{cf.KAFKASERVER: cf.KAFKAURL})
	if err != nil {
		return nil, err
	}

	return &kafkaP{
		p, cf,
	}, nil
}

func (k *kafkaP) SaveJobToKafka(job model.Job) error {
	d, err := json.Marshal(job)
	if err != nil {
		return err
	}

	for _, word := range []string{string(d)} {
		k.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.KAFKATOPIC, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	log.Println("The job event has been created successfully")
	return nil
}

func (k *kafkaP) Close() error {
	log.Printf("Closing confluentic produder")
	k.producer.Close()
	return nil
}
