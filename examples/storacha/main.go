//go:build !js

package main

import (
	"context"
	_ "embed"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/link"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/codec/json"
)

//go:embed schema.graphql
var schema string

//go:embed mutation.graphql
var mutation string

func main() {
	ctx := context.Background()

	links := link.NewStore(storage.NewMemory())
	db, err := capy.Open(ctx, links, schema)
	if err != nil {
		panic(err)
	}

	res, err := graphql.Execute(ctx, db, graphql.QueryParams{Query: mutation})
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

	err = links.Export(ctx, db.RootLink(), file)
	if err != nil {
		panic(err)
	}
}
