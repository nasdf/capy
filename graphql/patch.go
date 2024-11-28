package graphql

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	setPatch    = "set"
	appendPatch = "append"
)

func (e *executionContext) patchDocument(ctx context.Context, collection, id string, n datamodel.Node, value map[string]any) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(n.Length())
	if err != nil {
		return err
	}
	def, ok := e.schema.Types[collection]
	if !ok {
		return fmt.Errorf("invalid document type %s", collection)
	}
	for _, field := range def.Fields {
		if field.Name == "_link" || field.Name == "_id" {
			continue // ignore system fields
		}
		nv, err := n.LookupByString(field.Name)
		if _, ok := err.(datamodel.ErrNotExists); err != nil && !ok {
			return err
		}
		patch, ok := value[field.Name]
		if !ok && nv == nil {
			continue // ignore empty fields
		}
		na, err := ma.AssembleEntry(field.Name)
		if err != nil {
			return err
		}
		if ok {
			err = e.patchValue(ctx, field.Type, nv, patch, na)
		} else {
			err = na.AssignNode(nv)
		}
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

func (e *executionContext) patchValue(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	def, ok := e.schema.Types[typ.NamedType]
	if ok && def.Kind == ast.Object {
		return e.patchRelation(ctx, typ, n, value.(map[string]any), na)
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
		return e.assignValue(ctx, typ, patch[op], na)
	case appendPatch:
		return e.patchListAppend(ctx, typ, n, patch[op], na)
	default:
		return fmt.Errorf("invalid patch operation %s", op)
	}
}

func (e *executionContext) patchRelation(ctx context.Context, typ *ast.Type, n datamodel.Node, value map[string]any, na datamodel.NodeAssembler) error {
	if n == nil {
		return na.AssignNull()
	}
	id, err := n.AsString()
	if err != nil {
		return err
	}
	doc, err := e.tx.ReadDocument(ctx, typ.NamedType, id)
	if err != nil {
		return err
	}
	err = e.patchDocument(ctx, typ.NamedType, id, doc, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (e *executionContext) patchListAppend(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	var length int64
	var iter datamodel.ListIterator
	if n != nil {
		length = n.Length()
		iter = n.ListIterator()
	}
	// value is a single item or a list of items
	vals, ok := value.([]any)
	if !ok {
		vals = append(vals, value)
	}
	la, err := na.BeginList(length + int64(len(vals)))
	if err != nil {
		return err
	}
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
	for _, v := range vals {
		err = e.assignValue(ctx, typ.Elem, v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
