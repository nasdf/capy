package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
)

type DB struct {
	store  *core.Store
	schema *ast.Schema
}

func Open(ctx context.Context, storage storage.Storage, schema string) (*DB, error) {
	store, err := core.Open(ctx, storage, schema)
	if err != nil {
		return nil, err
	}
	s, err := graphql.GenerateSchema(schema)
	if err != nil {
		return nil, err
	}
	return &DB{
		store:  store,
		schema: s,
	}, nil
}

func Load(ctx context.Context, storage storage.Storage) (*DB, error) {
	schema, store, err := core.Load(ctx, storage)
	if err != nil {
		return nil, err
	}
	s, err := graphql.GenerateSchema(schema)
	if err != nil {
		return nil, err
	}
	return &DB{
		store:  store,
		schema: s,
	}, nil
}

func (db *DB) Store() *core.Store {
	return db.store
}

func (db *DB) Schema() *ast.Schema {
	return db.schema
}

func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (datamodel.Node, error) {
	return graphql.Execute(ctx, db.store, db.schema, params)
}
