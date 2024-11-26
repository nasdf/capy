package graphql

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/core"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
)

const (
	setPatch = "set"
)

func (e *executionContext) patchDocument(ctx context.Context, collection, id string, n schema.TypedNode, value any) error {
	if n.Kind() == datamodel.Kind_Link {
		return e.patchLink(ctx, collection, id, n, value)
	}
	nb := n.Prototype().NewBuilder()
	ma, err := nb.BeginMap(n.Length())
	if err != nil {
		return err
	}
	patch := value.(map[string]any)
	iter := n.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		key, err := k.AsString()
		if err != nil {
			return err
		}
		na, err := ma.AssembleEntry(key)
		if err != nil {
			return err
		}
		err = e.patchField(ctx, v.(schema.TypedNode), patch[key], na)
		if err != nil {
			return err
		}
	}
	err = ma.Finish()
	if err != nil {
		return err
	}
	lnk, err := e.store.Store(ctx, nb.Build())
	if err != nil {
		return err
	}
	rootNode, err := e.store.Load(ctx, e.rootLink, e.store.Prototype(core.RootTypeName))
	if err != nil {
		return err
	}
	rootPath := datamodel.ParsePath(collection + "/" + id)
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(lnk))
	if err != nil {
		return err
	}
	e.rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return err
	}
	return nil
}

func (b *executionContext) patchField(ctx context.Context, n schema.TypedNode, value any, na datamodel.NodeAssembler) error {
	if value == nil {
		return na.AssignNode(n)
	}
	if b.store.IsRelation(n.Type()) {
		id, err := n.AsString()
		if err != nil {
			return err
		}
		return b.patchRelation(ctx, n.Type().Name(), id, value)
	}
	patch := value.(map[string]any)
	if len(patch) != 1 {
		return fmt.Errorf("patch must contain exactly one operation")
	}
	var op string
	for k := range patch {
		op = k
	}
	switch op {
	case setPatch:
		return b.assignValue(ctx, n.Type(), patch[op], na)
	default:
		return fmt.Errorf("invalid patch operation %s", op)
	}
}

func (b *executionContext) patchLink(ctx context.Context, collection, id string, n schema.TypedNode, value any) error {
	lnk, err := n.AsLink()
	if err != nil {
		return err
	}
	obj, err := b.store.Load(ctx, lnk, core.Prototype(n))
	if err != nil {
		return err
	}
	return b.patchDocument(ctx, collection, id, obj.(schema.TypedNode), value)
}

func (b *executionContext) patchRelation(ctx context.Context, collection, id string, value any) error {
	rootLink, err := b.store.RootLink(ctx)
	if err != nil {
		return err
	}
	rootNode, err := b.store.Load(ctx, rootLink, b.store.Prototype(core.RootTypeName))
	if err != nil {
		return err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return err
	}
	linkNode, err := collectionNode.LookupByString(id)
	if err != nil {
		return err
	}
	return b.patchLink(ctx, collection, id, linkNode.(schema.TypedNode), value)
}
