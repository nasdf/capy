//go:build !js

package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"

	"github.com/99designs/gqlgen/graphql/playground"
)

//go:embed schema.graphql
var schema string

func main() {
	ctx := context.Background()

	address := "localhost:8080"
	if len(os.Args) >= 2 {
		address = os.Args[1]
	}

	db, err := capy.Open(ctx, storage.NewMemory(), schema)
	if err != nil {
		panic(err)
	}
	http.Handle("/", playground.Handler("Capy", "/query"))
	http.Handle("/query", graphql.Handler(db))

	fmt.Printf("Open a browser and navigate to %s\n", address)
	err = http.ListenAndServe(address, nil)
	if err != nil {
		panic(err)
	}
}
