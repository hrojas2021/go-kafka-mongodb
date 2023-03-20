package confluentic

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type producerHandler struct {
	kafkaP kafkaP
}

type kafkaP struct {
	*kafka.Producer
	*config.Configuration
}

func NewProducerHandler(cf *config.Configuration) (*producerHandler, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{cf.KAFKASERVER: cf.KAFKAURL})
	if err != nil {
		return nil, err
	}

	return &producerHandler{
		kafkaP: kafkaP{p, cf},
	}, nil
}

func (h *producerHandler) JobsPostHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("unable to read the payload ", err)
	}
	defer r.Body.Close()

	var job model.Job
	err = json.Unmarshal(body, &job)
	if err != nil {
		log.Fatal("unable to unmarshall the body ", err)
	}

	err = h.kafkaP.saveJobToKafka(job)
	if err != nil {
		log.Fatal("unable to produce the event  ", err)
	}

	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (k *kafkaP) saveJobToKafka(job model.Job) error {
	d, err := json.Marshal(job)
	if err != nil {
		return err
	}

	for _, word := range []string{string(d)} {
		k.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.KAFKATOPIC, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	log.Println("The job event has been created successfully")
	return nil
}

func (h *producerHandler) Close() error {
	h.kafkaP.Close()
	return nil
}
