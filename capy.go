package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/link"

	"github.com/ipld/go-ipld-prime/datamodel"
)

type DB struct {
	store *core.Store
	links *link.Store
}

// Open creates a new DB instance using the given store and schema.
func Open(ctx context.Context, links *link.Store, inputSchema string) (*DB, error) {
	rootNode, err := core.BuildRootNode(ctx, links, inputSchema)
	if err != nil {
		return nil, err
	}
	rootLink, err := links.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	store, err := core.NewStore(ctx, links, rootLink)
	if err != nil {
		return nil, err
	}
	return &DB{
		store: store,
		links: links,
	}, nil
}

func (db *DB) Store() *core.Store {
	return db.store
}

func (db *DB) Links() *link.Store {
	return db.links
}

func (db *DB) Execute(ctx context.Context, params graphql.QueryParams) (datamodel.Node, error) {
	tx, err := db.store.Transaction(ctx)
	if err != nil {
		return nil, err
	}
	data, err := graphql.Execute(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	rootLink, err := tx.Commit(ctx)
	if err != nil {
		return nil, err
	}
	err = db.store.Merge(ctx, rootLink)
	if err != nil {
		return nil, err
	}
	return data, nil
}
