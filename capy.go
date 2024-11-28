package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
)

type Capy struct {
	DB     *core.DB
	Schema *ast.Schema
}

func Open(ctx context.Context, storage storage.Storage, schema string) (*Capy, error) {
	db, err := core.Open(ctx, storage, schema)
	if err != nil {
		return nil, err
	}
	s, err := graphql.GenerateSchema(schema)
	if err != nil {
		return nil, err
	}
	return &Capy{
		DB:     db,
		Schema: s,
	}, nil
}

func (c *Capy) Execute(ctx context.Context, params graphql.QueryParams) (datamodel.Node, error) {
	return graphql.Execute(ctx, c.DB, c.Schema, params)
}
