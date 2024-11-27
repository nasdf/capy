package graphql

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/core"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
)

const (
	setPatch    = "set"
	appendPatch = "append"
)

func (e *executionContext) patchDocument(ctx context.Context, collection, id string, n schema.TypedNode, value any) error {
	nb := n.Prototype().NewBuilder()
	ma, err := nb.BeginMap(n.Length())
	if err != nil {
		return err
	}
	structType := n.Type().(*schema.TypeStruct)
	args := value.(map[string]any)
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
		typ := structType.Field(key).Type()
		err = e.patchField(ctx, typ, v, args[key], na)
		if err != nil {
			return err
		}
	}
	err = ma.Finish()
	if err != nil {
		return err
	}
	return e.tx.UpdateDocument(ctx, collection, id, nb.Build())
}

func (e *executionContext) patchField(ctx context.Context, t schema.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	if value == nil {
		return na.AssignNode(n)
	}
	collection, ok := core.RelationName(t)
	if ok {
		return e.patchRelation(ctx, collection, n, value)
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
		return e.assignValue(ctx, t, patch[op], na)
	case appendPatch:
		return e.patchListAppend(ctx, t, n, patch[op], na)
	default:
		return fmt.Errorf("invalid patch operation %s", op)
	}
}

func (e *executionContext) patchRelation(ctx context.Context, collection string, n datamodel.Node, value any) error {
	id, err := n.AsString()
	if err != nil {
		return err
	}
	doc, err := e.tx.ReadDocument(ctx, collection, id)
	if err != nil {
		return err
	}
	return e.patchDocument(ctx, collection, id, doc.(schema.TypedNode), value)
}

func (e *executionContext) patchListAppend(ctx context.Context, t schema.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	// value is a single item or a list of items
	vals, ok := value.([]any)
	if !ok {
		vals = append(vals, value)
	}
	la, err := na.BeginList(n.Length() + int64(len(vals)))
	if err != nil {
		return err
	}
	iter := n.ListIterator()
	for iter != nil && !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return err
		}
		err = la.AssembleValue().AssignNode(v)
		if err != nil {
			return err
		}
	}
	vt := t.(*schema.TypeList).ValueType()
	for _, v := range vals {
		err = e.assignValue(ctx, vt, v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
