package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/types"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
)

type DB struct {
	Store  *core.Store
	Types  *types.System
	Schema *ast.Schema
}

// New creates a new DB with the provided schema types in the given store.
func New(ctx context.Context, store *core.Store, schema string) (*DB, error) {
	system, err := types.NewSystem(schema)
	if err != nil {
		return nil, err
	}
	genSchema, err := graphql.GenerateSchema(schema)
	if err != nil {
		return nil, err
	}
	rootNode, err := system.RootNode()
	if err != nil {
		return nil, err
	}
	rootLink, err := store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	err = store.SetRootLink(ctx, rootLink)
	if err != nil {
		return nil, err
	}
	return &DB{
		Store:  store,
		Types:  system,
		Schema: genSchema,
	}, nil
}

// Open returns a DB with existing data from the rootLink in the given store.
func Open(ctx context.Context, store *core.Store) (*DB, error) {
	rootLink, err := store.RootLink(ctx)
	if err != nil {
		return nil, err
	}
	rootNode, err := store.Load(ctx, rootLink, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	schemaNode, err := rootNode.LookupByString(types.RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	schema, err := schemaNode.AsString()
	if err != nil {
		return nil, err
	}
	system, err := types.NewSystem(schema)
	if err != nil {
		return nil, err
	}
	genSchema, err := graphql.GenerateSchema(schema)
	if err != nil {
		return nil, err
	}
	return &DB{
		Store:  store,
		Types:  system,
		Schema: genSchema,
	}, nil
}

func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (datamodel.Node, error) {
	return graphql.Execute(ctx, db.Types, db.Store, db.Schema, params)
}
