package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"

	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/ipld/go-ipld-prime/storage/memstore"
)

//go:embed schema.graphql
var schema string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := "localhost:8080"
	if len(os.Args) >= 2 {
		address = os.Args[1]
	}

	db, err := capy.New(ctx, core.Open(ctx, &memstore.Store{}), schema)
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
