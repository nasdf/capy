package capy

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/link"
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
