package capy

import (
	"context"

	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/plan"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"

	"github.com/vektah/gqlparser/v2/ast"
)

type DB struct {
	// typeSys is the TypeSystem containing all user defined types.
	typeSys *schema.TypeSystem
	// schema contains the generated GraphQL schema.
	schema *ast.Schema
	// rootLnk is a link to the root data node.
	rootLnk datamodel.Link
	// store contains all db data.
	store data.Store
}

func New(ctx context.Context, schemaSrc string, store data.Store) (*DB, error) {
	// generate a TypeSystem from the user defined types
	typeSys, err := graphql.SpawnTypeSystem(schemaSrc)
	if err != nil {
		return nil, err
	}
	// generate a GraphQL schema containing all operations and types
	genSchema, err := graphql.GenerateSchema(typeSys)
	if err != nil {
		return nil, err
	}

	rootType := typeSys.TypeByName(data.RootTypeName)
	rootNode := bindnode.Prototype(nil, rootType).NewBuilder().Build()

	// create an empty root node
	rootLnk, err := store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}

	return &DB{
		store:   store,
		typeSys: typeSys,
		schema:  genSchema,
		rootLnk: rootLnk,
	}, nil
}

func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (any, error) {
	planNode, err := graphql.ParseQuery(db.schema, params)
	if err != nil {
		return nil, err
	}
	planner := plan.NewPlanner(db.store, *db.typeSys, db.rootLnk)
	rootLnk, res, err := planner.Execute(ctx, planNode)
	if err != nil {
		return nil, err
	}
	db.rootLnk = rootLnk
	return res, nil
}
