package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/confluentic"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/sarama"
)

func main() {
	cf := config.LoadViperConfig()
	handler, err := getBroker(cf)
	if err != nil {
		log.Fatal("unable to create a kafka producer handler ", err)
	}

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs", handler.JobsPostHandler).Methods("POST")

	log.Println("Server starting in localhost:", cf.PORT)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cf.PORT),
		Handler: router,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("Server is down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

}

func getBroker(cf *config.Configuration) (iface.ProducerHandler, error) {
	var broker iface.ProducerHandler
	var err error

	switch cf.BROKER {
	case config.Sarama:
		broker, err = sarama.NewProducerHandler(cf)
	default:
		broker, err = confluentic.NewProducerHandler(cf)
	}

	return broker, err
}
