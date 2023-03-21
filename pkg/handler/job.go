package handler

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type ProducerH struct {
	KafkaProducer iface.KafkaProducer
}

func NewProducerHandler(cf *config.Configuration, p iface.KafkaProducer) iface.ProducerHandler {
	return &ProducerH{
		KafkaProducer: p,
	}
}

func (p *ProducerH) JobsPostHandler(w http.ResponseWriter, r *http.Request) {
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

	err = p.KafkaProducer.SaveJobToKafka(job)
	if err != nil {
		log.Fatal("unable to produce the event  ", err)
	}

	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (p *ProducerH) Close() error {
	return p.KafkaProducer.Close()
}
