package sarama

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type kafkaP struct {
	producer sarama.SyncProducer
	*config.Configuration
}

func NewKafkaProducer(cf *config.Configuration) (iface.KafkaProducer, error) {
	servers := []string{cf.KAFKAURL}
	p, err := sarama.NewSyncProducer(servers, nil) // Phase III configs
	if err != nil {
		panic(err)
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

	msg := sarama.ProducerMessage{
		Topic: k.KAFKATOPIC,
		Value: sarama.ByteEncoder(d),
	}

	partition, offset, err := k.producer.SendMessage(&msg)
	if err != nil {
		return err
	}

	log.Printf("The job event has been created in partition %d and offset %d \n", partition, offset)
	return nil
}

func (k *kafkaP) Close() error {
	log.Printf("Closing sarama produder")
	return k.producer.Close()
}
