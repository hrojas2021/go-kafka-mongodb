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
	"github.com/hrojas2021/go-kafka-mongodb/pkg/handler"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/iface"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/confluentic"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/sarama"
	"github.com/hrojas2021/go-kafka-mongodb/pkg/kafka/segmentio"
)

// @contact.name   API Support
// @contact.url    http://www.swagger.io/support
// @contact.email  support@swagger.io

// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html
func main() {
	cf := config.LoadViperConfig()
	broker, err := getBroker(cf)
	if err != nil {
		log.Fatal("unable to create a kafka producer handler ", err)
	}

	handler := handler.NewProducerHandler(cf, broker)

	// docs.SwaggerInfo.Title = "Swagger Example API"
	// docs.SwaggerInfo.Description = "This is a sample server Petstore server."
	// docs.SwaggerInfo.Version = "1.0"
	// docs.SwaggerInfo.Host = "localhost:9600"
	// docs.SwaggerInfo.BasePath = "/v2"
	// docs.SwaggerInfo.Schemes = []string{"http", "https"}

	router := mux.NewRouter().StrictSlash(true)
	// router.PathPrefix("/swagger/").Handler(httpSwagger.Handler(
	// 	httpSwagger.URL("http://localhost:9600/swagger/doc.json"), //The url pointing to API definition
	// 	httpSwagger.DeepLinking(true),
	// 	httpSwagger.DocExpansion("none"),
	// 	httpSwagger.DomID("#swagger-ui"),
	// )).Methods(http.MethodGet)

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
	err = handler.Close()
	if err != nil {
		log.Fatalf("Error closing %s producer: %v", cf.BROKER, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cf.SERVERTIMEOUT)*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

func getBroker(cf *config.Configuration) (iface.KafkaProducer, error) {
	var broker iface.KafkaProducer
	var err error

	switch cf.BROKER {
	case config.Sarama:
		broker, err = sarama.NewKafkaProducer(cf)
	case config.Confluentic:
		broker, err = confluentic.NewKafkaProducer(cf)
	case config.Segmentio:
		broker, err = segmentio.NewKafkaProducer(cf)
	default:
		err = errors.New("the kafka broker was not found")
	}

	return broker, err
}
