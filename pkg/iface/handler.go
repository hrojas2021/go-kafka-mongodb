package iface

import "net/http"

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
