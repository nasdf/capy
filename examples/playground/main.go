package main

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/http"
)

//go:embed schema.graphql
var schema string

const address = "localhost:8080"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := capy.Open(ctx, data.NewMemStore(), schema)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Open a browser and navigate to %s\n", address)
	err = http.ListenAndServe(db, address)
	if err != nil {
		panic(err)
	}
}
