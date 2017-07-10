package main

import (
	"log"

	"github.com/takama/dbsync/pkg/server"
)

func main() {
	err := server.Run()
	if err != nil {
		log.Fatal("[SETUP:ERROR] ", err)
	}
}
