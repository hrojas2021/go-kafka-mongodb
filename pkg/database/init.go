package database

import (
	"context"
	"log"
	"time"

	"github.com/hrojas2021/go-kafka-mongodb/pkg/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	client *mongo.Client
	// session    *mgo.Session
	// collection *mgo.Collection
}

func Connect(cf *config.Configuration) (*DB, error) {
	// settings := &mgo.DialInfo{
	// 	Addrs:    []string{"localhost:27017"},
	// 	Timeout:  3 * time.Second,
	// 	Database: "animals",
	// 	Username: "",
	// 	Password: "",
	// }

	// s, err := mgo.Dial("mongodb://localhost:27017")
	// if err != nil {
	// 	return nil, err
	// }
	// c := s.DB(cf.MONGODB).C(cf.MONGOCOLLECTION)

	// return &DB{session: s, collection: c}, nil

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cf.MONGOTIMEOUT)*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return &DB{client: client}, nil
}
