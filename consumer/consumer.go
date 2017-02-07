package main

import (
	"bytes"
	"fmt"
	"os"

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

	fileOut, err := os.Create("out.txt")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer fileOut.Close()

	writeChan := make(chan []byte)
	closeChan := make(chan bool)
	go func() {
		for {
			select {
			case <-closeChan:
				close(writeChan)
				return
			case msg := <-writeChan:
				buf := bytes.NewBuffer(msg)
				buf.Write([]byte("\r\n"))
				_, err = fileOut.Write(buf.Bytes())
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}()

	err = client.Subscribe("infrastructure.metrics", func(msg []byte) {
		// fmt.Println(string(msg))
		writeChan <- msg
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// err = client.Unsubscribe("infrastructure", subsID)
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
	// os.Exit(0)
}
