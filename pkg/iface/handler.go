package iface

import (
	"net/http"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

type Closer interface {
	Close() error
}
type ConsumerHandler interface {
	ReadMessagesFromKafka() error
	Subscribe() error
	Closer
}

type ProducerHandler interface {
	JobsPostHandler(w http.ResponseWriter, r *http.Request)
	Closer
}

type KafkaProducer interface {
	SaveJobToKafka(job model.Job) error
	Closer
}
