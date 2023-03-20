package segmentio

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
	"github.com/segmentio/kafka-go"
)

type producerHandler struct {
	kafkaP kafkaP
}

type kafkaP struct {
	*kafka.Writer
	*config.Configuration
	counter int
}

func NewProducerHandler(cf *config.Configuration) (*producerHandler, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{cf.KAFKAURL},
		Topic:   cf.KAFKATOPIC,
	})

	return &producerHandler{
		kafkaP: kafkaP{w, cf, 1},
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

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("key-%d", k.counter)),
		Value: d,
	}

	err = k.WriteMessages(context.Background(), msg)
	if err != nil {
		return err
	}

	return nil
}

func (h *producerHandler) Close() error {
	return h.kafkaP.Close()
}
