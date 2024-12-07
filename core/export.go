package core

import (
	"context"
	"io"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// Export writes a CAR containing the DAG starting from the given root link to the given io.Writer.
func Export(ctx context.Context, store *Store, rootLink datamodel.Link, out io.Writer) error {
	cid := rootLink.(cidlink.Link).Cid
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	w, err := car.NewSelectiveWriter(ctx, &store.lsys, cid, sel.Node())
	if err != nil {
		return err
	}
	_, err = w.WriteTo(out)
	return err
}
