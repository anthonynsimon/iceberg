package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anthonynsimon/iceberg"
)

func main() {
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

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 10000; j++ {
				time.Sleep(500 * time.Millisecond)
				err = client.Publish("infrastructure.metrics", []byte(fmt.Sprintf(
					"metrics message %06d from goroutine %04d", j, i)))
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}
