package capy

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/plan"
	"github.com/nasdf/capy/types"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"

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

func Open(ctx context.Context, schemaSrc string, store data.Store) (*DB, error) {
	// generate a TypeSystem from the user defined types
	typeSys, err := types.SpawnTypeSystem(schemaSrc)
	if err != nil {
		return nil, err
	}
	// generate a GraphQL schema containing all operations and types
	genSchema, err := graphql.GenerateSchema(typeSys)
	if err != nil {
		return nil, err
	}

	rootType := typeSys.TypeByName(types.RootTypeName)
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
	planner := plan.NewPlanner(db.store, db.typeSys, db.rootLnk)
	rootLnk, res, err := planner.Execute(ctx, planNode)
	if err != nil {
		return nil, err
	}
	db.rootLnk = rootLnk
	return res, nil
}

// Export exports all of the data into a content addressable archive file.
func (db *DB) Export(ctx context.Context, path string) error {
	lsys := db.store.LinkSystem()
	root, err := cid.Decode(db.rootLnk.String())
	if err != nil {
		return err
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
	return car.TraverseToFile(ctx, &lsys, root, sel, path)
}
