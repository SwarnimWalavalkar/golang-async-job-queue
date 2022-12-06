package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SwarnimWalavalkar/go-async-job-queue/queue"
	"github.com/go-redis/redis/v8"
)

var STREAM_NAME = "test_stream"

func main() {
	redisDB := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})

	go StartConsumer(redisDB)
}

type RedisStreamProcessor struct {
	Redis *redis.Client
}

func (redisStreamProcessor *RedisStreamProcessor) Process(job interface{}) {
	if data, ok := job.(redis.XMessage); ok {
		fmt.Printf("Received Data:\n%#v", data.Values)
	} else {
		log.Println("Unknown data type")
	}
}

func StartConsumer (rdb *redis.Client) {
	redisStream := RedisStreamProcessor {
		Redis: rdb,
	}

	jobQueue :=  queue.NewQueue(5, 10, redisStream.Process)

	go jobQueue.StartWorkers()

	id := "0"

	for {
		var ctx = context.Background();

		data, err := rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{STREAM_NAME, id}, Count: 4, Block: 0}).Result()

		if err != nil {
			log.Fatal(err)
		}
		
		for _, result := range data {
			for _, message := range result.Messages {
				jobQueue.EnqueueJobBlocking(message)

				id = message.ID
			}
		}
	}
}