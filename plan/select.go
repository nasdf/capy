package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type selectNode struct {
	// sel contains the fields that will be selected.
	sel selector.Selector
	// ops is a list of nodes to execute before the select.
	ops []Node
}

// Select returns a new Node that returns the selected fields when executed.
func Select(sel selector.Selector, ops ...Node) Node {
	return &selectNode{
		sel: sel,
		ops: ops,
	}
}

func (n *selectNode) Execute(ctx context.Context, store Storage) (any, error) {
	for _, o := range n.ops {
		_, err := o.Execute(ctx, store)
		if err != nil {
			return nil, err
		}
	}
	root, err := store.Load(ctx, store.GetRootLink())
	if err != nil {
		return nil, err
	}

	mapper := NewResult()
	err = store.Traversal(ctx).WalkMatching(root, n.sel, func(p traversal.Progress, n datamodel.Node) error {
		return mapper.Set(p.Path, n)
	})
	return mapper, err
}
