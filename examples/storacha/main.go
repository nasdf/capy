//go:build !js

package main

import (
	"context"
	_ "embed"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/codec/json"
)

//go:embed schema.graphql
var schema string

//go:embed mutation.graphql
var mutation string

func main() {
	ctx := context.Background()

	db, err := capy.Open(ctx, storage.NewMemory(), schema)
	if err != nil {
		panic(err)
	}

	res, err := db.Execute(ctx, graphql.QueryParams{Query: mutation})
	if err != nil {
		panic(err)
	}

	err = json.Encode(res, os.Stdout)
	if err != nil {
		panic(err)
	}

	file, err := os.Create("export.car")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	err = db.Export(ctx, file)
	if err != nil {
		panic(err)
	}
}
