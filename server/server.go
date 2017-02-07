package main

import (
	"fmt"
	"os"

	"github.com/anthonynsimon/iceberg"
)

func main() {
	s, err := iceberg.NewServer(":7260")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s.Run()
}
