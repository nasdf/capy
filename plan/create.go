package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/nasdf/capy/node"
)

type createNode struct {
	// collection is the name of collection the created object belongs to.
	collection string
	// input contains the data used to create the object
	input any
}

// Create returns a new Node that creates collection objects when executed.
func Create(collection string, input any) Node {
	return &createNode{
		collection: collection,
		input:      input,
	}
}

func (n *createNode) Execute(ctx context.Context, store Storage) (any, error) {
	builder := node.NewBuilder(store)
	// TODO: limit the query to the link returned from the builder
	// the following select node should only return results containing the created object
	// it should be possible to tell which indexes were created during this step and use
	// a range selector to match against them
	_, err := builder.Build(ctx, store.TypeSystem().TypeByName(n.collection), n.input)
	if err != nil {
		return nil, err
	}
	root, err := store.Load(ctx, store.GetRootLink())
	if err != nil {
		return nil, err
	}
	for col, lnks := range builder.Links() {
		for _, lnk := range lnks {
			path := datamodel.ParsePath(col).AppendSegmentString("-")
			root, err = store.Traversal(ctx).FocusedTransform(root, path, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
				return basicnode.NewLink(lnk), nil
			}, true)
			if err != nil {
				return nil, err
			}
		}
	}
	rootLnk, err := store.Store(ctx, root)
	if err != nil {
		return nil, err
	}
	store.SetRootLink(rootLnk)
	return nil, nil
}
