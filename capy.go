package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/link"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// Open creates a new DB instance using the given store and schema.
func Open(ctx context.Context, links *link.Store, inputSchema string) (*core.Store, error) {
	rootNode, err := core.BuildInitialRootNode(ctx, links, inputSchema)
	if err != nil {
		return nil, err
	}
	rootLink, err := links.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	return core.NewStore(ctx, links, rootLink)
}

func Execute(ctx context.Context, store *core.Store, params graphql.QueryParams) (datamodel.Node, error) {
	tx, err := store.Collections(ctx)
	if err != nil {
		return nil, err
	}
	data, err := graphql.Execute(ctx, tx, store.Schema(), params)
	if err != nil {
		return nil, err
	}
	rootLink, err := tx.Commit(ctx)
	if err != nil {
		return nil, err
	}
	err = store.Merge(ctx, rootLink)
	if err != nil {
		return nil, err
	}
	return data, nil
}
