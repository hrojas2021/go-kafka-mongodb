package iface

import "net/http"

type ConsumerHandler interface {
	ReadMessagesFromKafka() error
	Close() error
	Subscribe() error
}

type ProducerHandler interface {
	JobsPostHandler(w http.ResponseWriter, r *http.Request)
}
