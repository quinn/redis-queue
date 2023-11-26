package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

//var name string = "worker1"

type Ping struct {
	Message string `json:"message"`
	Date    string `json:"date"`
}

type Pong struct {
	Reply string `json:"message"`
	Date  string `json:"date"`
}

type Queue struct {
	Ctx     context.Context
	Rdb     *redis.Client
	Wg      *sync.WaitGroup
	ErrChan chan error
}

func queue[T interface{}](q *Queue, name string, handler func(T) error) {
	processingName := fmt.Sprintf("%s-processing", name)
	restoredMessages := 0

	for {
		_, err := q.Rdb.RPopLPush(q.Ctx, processingName, name).Result()

		if err != nil {
			if err == redis.Nil {
				break
			}
			q.ErrChan <- fmt.Errorf("failed to restore queue `%s` from `%s`: %s", name, processingName, err)
			return
		}

		restoredMessages++
	}

	if restoredMessages > 0 {
		fmt.Printf("Restored %d messages from `%s` to `%s`\n", restoredMessages, processingName, name)
	}

	q.Wg.Add(1)
	go func() {
		for {
			result, err := q.Rdb.BRPopLPush(q.Ctx, name, processingName, 5*time.Second).Result()

			if err != nil {
				if err == redis.Nil {
					fmt.Printf("Queue `%s` is empty\n", name)
					continue
				} else {
					q.ErrChan <- err
					break
				}
			}

			var payload T

			err = json.Unmarshal([]byte(result), &payload)
			if err != nil {
				q.ErrChan <- err
				break
			}

			err = handler(payload)
			if err != nil {
				q.ErrChan <- err
				break
			}

			_, err = q.Rdb.LRem(q.Ctx, processingName, 1, result).Result()
			if err != nil {
				q.ErrChan <- err
				break
			}
		}

		q.Wg.Done()
	}()
}

func main() {
	url := "redis://localhost:6379"
	opts, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(opts)

	// wait group
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		Ctx:     ctx,
		Rdb:     rdb,
		Wg:      &sync.WaitGroup{},
		ErrChan: make(chan error, 1),
	}

	defer q.Wg.Wait()

	go func() {
		for err := range q.ErrChan {
			fmt.Printf("Channel failed. Cancelling. Error: %s\n", err)
			cancel()
		}
	}()

	queue(q, "ping", func(p Ping) error {
		fmt.Printf("Ping Message: %s\n", p.Message)

		pong := Pong{
			Reply: "pong",
			Date:  time.Now().Format(time.RFC3339),
		}

		payload, err := json.Marshal(pong)
		if err != nil {
			return err
		}

		q.Rdb.LPush(q.Ctx, "pong", payload)
		return nil
	})

	queue(q, "pong", func(p Pong) error {
		fmt.Printf("Pong Reply: %s\n", p.Reply)

		ping := Ping{
			Message: "ping",
			Date:    time.Now().Format(time.RFC3339),
		}

		payload, err := json.Marshal(ping)
		if err != nil {
			return err
		}

		q.Rdb.LPush(q.Ctx, "ping", payload)
		return nil
	})
}
