package main

import (
	"fmt"
	"os"
	"sync"

	"time"

	"encoding/binary"

	"github.com/anthonynsimon/iceberg"
)

func main() {
	concurrency := 1
	messagesPerGoroutine := 100
	topic := "infrastructure.metrics"
	addr := "localhost:7260"

	var latencies []time.Duration
	wg := &sync.WaitGroup{}

	server, err := iceberg.NewServer(":7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	go func() {
		server.Run()
	}()

	wg.Add(1)
	go func() {
		consumer, err := iceberg.NewClient(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = consumer.Connect(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		msgsExpected := messagesPerGoroutine * concurrency

		err = consumer.Subscribe(topic, func(msg []byte) {
			received := time.Now()
			sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(msg)))
			latencies = append(latencies, received.Sub(sentAt))
			msgsExpected--
			if msgsExpected <= 0 {
				wg.Done()
			}
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			producer, err := iceberg.NewClient(addr)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			err = producer.Connect(addr)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			for j := 0; j < messagesPerGoroutine; j++ {
				var encoded [8]byte
				binary.BigEndian.PutUint64(encoded[:], uint64(time.Now().UnixNano()))
				err = producer.Publish(topic, encoded[:])
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
			wg.Done()
		}(i, wg)
	}

	wg.Wait()

	var average time.Duration
	for _, lat := range latencies {
		average += lat
	}
	if len(latencies) > 1 {
		average /= time.Duration(len(latencies))
	}

	fmt.Printf("--------------\n"+
		"concurrency: %d\n"+
		"messages per goroutine: %d\n"+
		"average latency from publisher to subscriber: %s\n"+
		"total samples (messages): %d\n"+
		"--------------\n",
		concurrency, messagesPerGoroutine, average, len(latencies))

	server.Shutdown()
}
