package main

import (
	"fmt"
	"os"

	"sync"

	"math/rand"

	"github.com/anthonynsimon/iceberg"
)

type data struct {
	d string
}

func main() {
	client, err := iceberg.NewClient("localhost:7160")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	subsID, err := client.Subscribe("infrastructure", func(msg []byte) {
		fmt.Println(string(msg))
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100000; j++ {
				err = client.Publish("infrastructure", []byte(fmt.Sprintf("metrics message %d", rand.Intn(1<<32))))
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	err = client.Unsubscribe("infrastructure", subsID)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = client.Publish("infrastructure", []byte("another message"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Exit(0)
}
