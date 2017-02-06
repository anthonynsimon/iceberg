package main

import (
	"fmt"
	"math/rand"
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
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 10000; j++ {
				time.Sleep(10 * time.Millisecond)
				err = client.Publish("infrastructure.metrics", []byte(fmt.Sprintf("metrics message %d", rand.Intn(1<<32))))
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
