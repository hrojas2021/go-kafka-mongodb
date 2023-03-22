package database

import (
	"context"
	"log"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

func (db *DB) SaveJob(job *model.Job) error {
	log.Println("Saving job to MongoDB")
	coll := db.client.Database("dockerKafka").Collection("jobs")
	_, err := coll.InsertOne(context.Background(), job)
	if err != nil {
		return err
	}
	return nil
}
