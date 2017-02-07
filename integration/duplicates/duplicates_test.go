package duplicates

import (
	"fmt"
	"os"
	"sync"

	"testing"

	"github.com/anthonynsimon/iceberg"
)

func TestDuplicates(t *testing.T) {
	wg := &sync.WaitGroup{}

	go func() {
		s := iceberg.NewServer(":7260")
		s.Run()
	}()

	received := make(map[string]int)

	go consume("infrastructure.metrics", received)

	concurrency := 1000
	messagesPerGoroutine := 1000

	wg.Add(concurrency)

	go produce("infrastructure.metrics", wg, concurrency, messagesPerGoroutine)

	wg.Wait()

	for k, v := range received {
		if v > 1 {
			t.Errorf("duplicate entry found: %s", k)
		}
	}
}

func produce(topic string, wg *sync.WaitGroup, concurrency, messagesPerGoroutine int) {
	client, err := iceberg.NewClient("localhost:7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = client.Connect("localhost:7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				err = client.Publish(topic, []byte(fmt.Sprintf("%06d %04d", j, i)))
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
			wg.Done()
		}(i)
	}

}

func consume(topic string, entries map[string]int) {
	client, err := iceberg.NewClient("localhost:7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = client.Connect("localhost:7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = client.Subscribe(topic, func(msg []byte) {
		entries[string(msg)]++
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
