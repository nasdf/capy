package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type selectNode struct {
	// sel contains the fields that will be selected.
	sel selector.Selector
	// ops is a list of nodes to execute before the select.
	ops []Node
	// res is the type for the select result.
	res schema.Type
}

// Select returns a new Node that returns the selected fields when executed.
func Select(sel selector.Selector, res schema.Type, ops ...Node) Node {
	return &selectNode{
		sel: sel,
		res: res,
		ops: ops,
	}
}

func (n *selectNode) Execute(ctx context.Context, store Storage) (datamodel.Node, error) {
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

	resultNode := bindnode.Prototype(nil, n.res).NewBuilder().Build()
	resultMapper := NewMapper()

	err = store.Traversal(ctx).WalkMatching(root, n.sel, func(p traversal.Progress, n datamodel.Node) error {
		rootPath := resultMapper.Path(p.Path)
		rootTransform := func(_ traversal.Progress, _ datamodel.Node) (datamodel.Node, error) {
			return n, nil
		}
		resultNode, err = store.Traversal(ctx).FocusedTransform(resultNode, rootPath, rootTransform, true)
		return err
	})
	return resultNode, err
}
