package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/nasdf/capy/node"
)

type mutationNode struct {
	req Request
}

func Mutation(req Request) Node {
	return &mutationNode{
		req: req,
	}
}

func (n *mutationNode) Execute(ctx context.Context, store Storage) (any, error) {
	builder := node.NewBuilder(store)
	for i, f := range n.req.Fields {
		if !strings.HasPrefix(f.Name, "create") {
			return nil, fmt.Errorf("unsupported operation %s", f.Name)
		}
		collection := strings.TrimPrefix(f.Name, "create")
		objectType := store.TypeSystem().TypeByName(collection)
		objectLink, err := builder.Build(ctx, objectType, f.Arguments["input"])
		if err != nil {
			return nil, err
		}
		n.req.Fields[i].Name = collection
		n.req.Fields[i].Arguments["id"] = objectLink.String()
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

	return Query(n.req).Execute(ctx, store)
}
