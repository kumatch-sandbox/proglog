package main

import (
	"log"

	"github.com/kumatch-sandbox/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
