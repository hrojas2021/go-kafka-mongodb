package database

import (
	"context"
	"log"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/model"
)

func (db *DB) SaveJob(job *model.Job) error {
	// return db.collection.Insert(job)
	log.Println("Saving job to MongoDB")
	col := db.client.Database("kafka").Collection("jobs")
	_, err := col.InsertOne(context.Background(), job)
	if err != nil {
		return err
	}
	return nil
}
