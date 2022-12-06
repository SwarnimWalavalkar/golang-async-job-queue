package queue

import (
	"log"
	"sync"
)

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