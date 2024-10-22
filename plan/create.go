package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/traversal"
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
	root, err := n.createObject(ctx, store)
	if err != nil {
		return nil, err
	}
	rootLnk, err := store.Store(ctx, root)
	if err != nil {
		return nil, err
	}
	store.SetRootLink(rootLnk)
	return nil, nil
}

func (n *createNode) createObject(ctx context.Context, store Storage) (datamodel.Node, error) {
	// TODO: this does not handle related objects
	// create a custom node builder that can handle it
	objType := store.TypeSystem().TypeByName(n.collection)
	objNode, err := fluent.Reflect(bindnode.Prototype(nil, objType), n.input)
	if err != nil {
		return nil, err
	}
	lnk, err := store.Store(ctx, objNode)
	if err != nil {
		return nil, err
	}
	root, err := store.Load(ctx, store.GetRootLink())
	if err != nil {
		return nil, err
	}
	path := datamodel.ParsePath(n.collection).AppendSegmentString("-")
	return store.Traversal(ctx).FocusedTransform(root, path, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
		return basicnode.NewLink(lnk), nil
	}, true)
}
