package client

import (
	"time"
	"context"

	"encoding/json"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/broderickhyman/albiondata-client/log"
)

// "mongodb://localhost:27017"

type mongoDBUploader struct {
	url string
}

func newMongoDBUploader(url string) uploader {

	log.Infof("Creating MongoDB uploader")

	return &mongoDBUploader{
		url: url,
	}
}

func (u *mongoDBUploader) sendToIngest(body []byte, topic string) {
	// if err := u.nc.Publish(topic, body); err != nil {
	// 	log.Errorf("Error while sending ingest to nats with data: %v", err)
	// }
	if(topic == "marketorders.ingest") {

		// connect
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(u.url))

		defer func() {
			if err = client.Disconnect(ctx); err != nil {
				panic(err)
			}
		}()

		// this is not great but fine for poc
		var result map[string]interface{}
		json.Unmarshal(body, &result)

		//
		orders := result["Orders"].([]interface{})
		log.Infof("Writing %d orders to MongoDB", len(orders))

		//
		collection := client.Database("albion").Collection("orders")

		for _, order := range orders {
			// Each value is an interface{} type, that is type asserted as a string
			b, err := bson.Marshal(order)
			if(err == nil) {
				_, err := collection.InsertOne(ctx, b)
				if(err != nil) {
					log.Warnf("Couldn't write order: %s", err)
				}
			} else {
				log.Warnf("Couldn't marshall order to bson: %s", err)
			}
		}
	}
}