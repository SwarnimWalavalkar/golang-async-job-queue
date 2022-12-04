package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/go-redis/redis/v8"
)

var STREAM_NAME = "test_stream"

type JobCallback func (job interface{})

type Queue struct {
	Workers int
	Capacity int
	JobQueue chan interface{}
	Wg *sync.WaitGroup
	QuitChan chan struct{}
	JobCallback JobCallback
}

func NewQueue(workers int, capacity int, callback JobCallback) Queue {
	var wg sync.WaitGroup
	jobQueue := make(chan interface{}, capacity)
	quit := make(chan struct{})

	return Queue{
		Workers: workers,
		JobQueue: jobQueue,
		JobCallback: callback,
		Wg: &wg,
		QuitChan: quit,
	}
}

func (q *Queue) Stop() {
	q.QuitChan <- struct{}{}
}

func (q *Queue) EnqueueJobNonBlocking(job interface{}) bool {
	select {
	case q.JobQueue <- job:
		return true
	default:
		return false
	}
}

func (q *Queue) EnqueueJobBlocking(job interface{}) {
	q.JobQueue <- job
}

func (q *Queue) worker() {
	defer q.Wg.Done()

	for {
		select {
		case <- q.QuitChan:
			log.Println("Terminating all workers")
			return
		case job := <- q.JobQueue:
			q.JobCallback(job)
		}
	}
}

func (q *Queue) StartWorkers() {
	for i := 0; i < q.Workers; i++ {
		q.Wg.Add(1)
		go q.worker()
	}
	q.Wg.Wait()
}

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

	log.Println("HERE!")

	queue :=  NewQueue(5, 10, redisStream.Process)

	go queue.StartWorkers()

	id := "0"

	for {
		var ctx = context.Background();

		data, err := rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{STREAM_NAME, id}, Count: 4, Block: 0}).Result()

		if err != nil {
			log.Fatal(err)
		}
		
		for _, result := range data {
			for _, message := range result.Messages {
				queue.EnqueueJobBlocking(message)

				id = message.ID
			}
		}
	}
}