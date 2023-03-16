package main

import (
	"log"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/database"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/confluentic"
)

func main() {

	cf := config.LoadViperConfig()
	_, err := database.Connect(cf)
	if err != nil {
		log.Fatal("unable to connecto to mongoDB ", err)
	}
	handler, err := confluentic.NewConsumerHandler(cf, nil)
	if err != nil {
		log.Fatal("unable to create a kafka consumer handler ", err)
	}
	err = handler.Subscribe()
	if err != nil {
		log.Fatal("unable to subscribe the topics ", err)
	}

	err = handler.ReadMessagesFromKafka()
	if err != nil {
		log.Fatal("unable to read messages from Kafka ", err)
	}

	err = handler.Close()
	if err != nil {
		log.Fatal("unable to close the consumer ", err)
	}
}
