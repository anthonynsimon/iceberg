package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anthonynsimon/iceberg"
)

type data struct {
	d string
}

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
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100000; j++ {
				time.Sleep(150 * time.Millisecond)
				err = client.Publish("infrastructure.metrics", []byte(fmt.Sprintf(
					"metrics message %d from goroutine %d", j, i)))
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
