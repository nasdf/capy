package capy

import (
	"context"
	"io"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/graphql/schema_gen"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
)

// RootLinkKey is the key for the head root link.
const RootLinkKey = "root"

type DB struct {
	store    storage.Storage
	links    *core.Store
	schema   *ast.Schema
	rootLink datamodel.Link
}

// Open creates a new DB instance using the given store and schema.
func Open(ctx context.Context, store storage.Storage, inputSchema string) (*DB, error) {
	links := core.NewStore(store)

	schema, err := schema_gen.Execute(inputSchema)
	if err != nil {
		return nil, err
	}
	rootNode, err := core.BuildRootNode(ctx, links, inputSchema)
	if err != nil {
		return nil, err
	}
	rootLink, err := links.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	err = store.Put(ctx, RootLinkKey, []byte(rootLink.String()))
	if err != nil {
		return nil, err
	}
	return &DB{
		store:    store,
		links:    links,
		schema:   schema,
		rootLink: rootLink,
	}, nil
}

func (db *DB) Export(ctx context.Context, out io.Writer) error {
	return core.Export(ctx, db.links, db.rootLink, out)
}

func (db *DB) Dump(ctx context.Context) (map[string][]string, error) {
	return core.Dump(ctx, db.links, db.rootLink)
}

func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (datamodel.Node, error) {
	tx, err := core.NewTransaction(ctx, db.links, db.schema, db.rootLink)
	if err != nil {
		return nil, err
	}
	data, err := graphql.Execute(ctx, tx, db.schema, params)
	if err != nil {
		return nil, err
	}
	rootLink, err := tx.Commit(ctx)
	if err != nil {
		return nil, err
	}
	db.rootLink = rootLink
	return data, nil
}
