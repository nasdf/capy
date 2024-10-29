package capy

import (
	"context"
	"io"

	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/plan"
	"github.com/nasdf/capy/types"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"

	"github.com/vektah/gqlparser/v2/ast"
)

type DB struct {
	// typeSys is the TypeSystem containing all user defined types.
	typeSys *schema.TypeSystem
	// schema contains the generated GraphQL schema.
	schema *ast.Schema
	// rootLink is a link to the root data node.
	rootLink datamodel.Link
	// store contains all db data.
	store *data.Store
}

// Open creates a new DB with the provided schema types in the given store.
func Open(ctx context.Context, store *data.Store, schemaString string) (*DB, error) {
	typeSys, err := types.SpawnTypeSystem(schemaString)
	if err != nil {
		return nil, err
	}
	schema, err := graphql.GenerateSchema(typeSys)
	if err != nil {
		return nil, err
	}
	// create a new root with the provided schema
	nb := bindnode.Prototype(nil, typeSys.TypeByName(types.RootTypeName)).NewBuilder()
	mb, err := nb.BeginMap(1)
	if err != nil {
		return nil, err
	}
	na, err := mb.AssembleEntry(types.RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	err = na.AssignString(schemaString)
	if err != nil {
		return nil, err
	}
	rootLink, err := store.Store(ctx, nb.Build())
	if err != nil {
		return nil, err
	}
	return &DB{
		store:    store,
		typeSys:  typeSys,
		schema:   schema,
		rootLink: rootLink,
	}, nil
}

// Load returns a DB with existing data from the rootLink in the given store.
func Load(ctx context.Context, store *data.Store, rootLink datamodel.Link) (*DB, error) {
	rootNode, err := store.Load(ctx, rootLink, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	schemaNode, err := rootNode.LookupByString(types.RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	inputSchema, err := schemaNode.AsString()
	if err != nil {
		return nil, err
	}
	typeSys, err := types.SpawnTypeSystem(inputSchema)
	if err != nil {
		return nil, err
	}
	schema, err := graphql.GenerateSchema(typeSys)
	if err != nil {
		return nil, err
	}
	return &DB{
		store:    store,
		typeSys:  typeSys,
		schema:   schema,
		rootLink: rootLink,
	}, nil
}

// Execute runs the operations in the given query.
func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (any, error) {
	planNode, err := graphql.ParseQuery(db.schema, params)
	if err != nil {
		return nil, err
	}
	planner := plan.NewPlanner(db.store, db.typeSys, db.rootLink)
	rootLnk, res, err := planner.Execute(ctx, planNode)
	if err != nil {
		return nil, err
	}
	db.rootLink = rootLnk
	return res, nil
}

func (db *DB) Export(ctx context.Context, out io.Writer) error {
	return db.store.Export(ctx, db.rootLink, out)
}
