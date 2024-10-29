package main

import (
	"context"
	_ "embed"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/http"
)

//go:embed schema.graphql
var schema string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := capy.Open(ctx, data.NewMemStore(), schema)
	if err != nil {
		panic(err)
	}

	err = http.ListenAndServe(db, ":8080")
	if err != nil {
		panic(err)
	}
}
