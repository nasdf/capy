//go:build !js

package main

import (
	"context"
	_ "embed"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/codec/json"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

//go:embed schema.graphql
var schema string

//go:embed mutation.graphql
var mutation string

func main() {
	ctx := context.Background()
	c, err := capy.Open(ctx, storage.NewMemory(), schema)
	if err != nil {
		panic(err)
	}
	res, err := c.Execute(ctx, graphql.QueryParams{Query: mutation})
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

	rootLink := c.DB.RootLink()
	root, err := cid.Decode(rootLink.String())
	if err != nil {
		panic(err)
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	w, err := car.NewSelectiveWriter(ctx, c.DB.LinkSystem(), root, sel.Node())
	if err != nil {
		panic(err)
	}
	_, err = w.WriteTo(file)
	if err != nil {
		panic(err)
	}
}
