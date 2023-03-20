package sarama

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type producerHandler struct {
	kafkaP kafkaP
}

type kafkaP struct {
	sarama.SyncProducer
	*config.Configuration
}

func NewProducerHandler(cf *config.Configuration) (*producerHandler, error) {
	servers := []string{cf.KAFKAURL}
	p, err := sarama.NewSyncProducer(servers, nil) // Phase III configs
	if err != nil {
		panic(err)
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

	msg := sarama.ProducerMessage{
		Topic: k.KAFKATOPIC,
		Value: sarama.ByteEncoder(d),
	}

	partition, offset, err := k.SendMessage(&msg)
	if err != nil {
		return err
	}

	log.Printf("The job event has been created in partition %d and offset %d \n", partition, offset)
	return nil
}

func (h *producerHandler) Close() error {
	return h.kafkaP.Close()
}
