//go:build !js

package core

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// ExportCAR exports a content addressable archive containing all of the data within the store.
func ExportCAR(ctx context.Context, store *Store, out io.Writer) error {
	rootLink, err := store.RootLink(ctx)
	if err != nil {
		return err
	}
	root, err := cid.Decode(rootLink.String())
	if err != nil {
		return err
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	// write the contents encoded as a CAR
	w, err := car.NewSelectiveWriter(ctx, &store.links, root, sel.Node())
	if err != nil {
		return err
	}
	_, err = w.WriteTo(out)
	return err
}
