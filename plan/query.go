package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal"
)

type queryNode struct {
	// sel contains the fields that will be selected.
	req Request
}

// Query returns a new node that returns the selected fields when executed.
func Query(req Request) Node {
	return &queryNode{
		req: req,
	}
}

func (n *queryNode) Execute(ctx context.Context, store Storage) (any, error) {
	root, err := store.Load(ctx, store.GetRootLink())
	if err != nil {
		return nil, err
	}
	sel, err := n.req.selectorSpec().Selector()
	if err != nil {
		return nil, err
	}
	res := NewResult()
	err = store.Traversal(ctx).WalkMatching(root, sel, func(p traversal.Progress, n datamodel.Node) error {
		return res.Set(p.Path, n)
	})
	return res, err
}
