package main

import (
	"log"

	"github.com/takama/dbsync/pkg/server"
)

func main() {
	srv, err := server.Setup()
	if err != nil {
		log.Fatal("[SETUP:ERROR] ", err)
	}

	err = srv.ListenAndServe()
	if err != nil {
		log.Fatal("Could not listen: ", err)
	}
}
