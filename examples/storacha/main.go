package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/graphql"
)

//go:embed schema.graphql
var schema string

//go:embed mutation.graphql
var mutation string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := capy.Open(ctx, data.NewMemStore(), schema)
	if err != nil {
		panic(err)
	}

	res, err := db.Execute(ctx, graphql.QueryParams{
		Query: mutation,
	})
	if err != nil {
		panic(err)
	}

	out, err := json.MarshalIndent(res, "", "\t")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))

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
