package graphql

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"
)

const (
	equalFilter          = "_eq"
	notEqualFilter       = "_neq"
	greaterFilter        = "_gt"
	greaterOrEqualFilter = "_gte"
	lessFilter           = "_lt"
	lessOrEqualFilter    = "_lte"
	inFilter             = "_in"
	notInFilter          = "_nin"
	andFilter            = "_and"
	orFilter             = "_or"
	notFilter            = "_not"
	allFilter            = "_all"
	anyFilter            = "_any"
	noneFilter           = "_none"
)

func (e *executionContext) filterNode(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	if f == nil {
		return true, nil
	}
	if n.Kind() == datamodel.Kind_Link {
		return e.filterLink(ctx, n, f)
	}
	if e.system.IsRelation(n.Type()) {
		id, err := n.AsString()
		if err != nil {
			return false, err
		}
		return e.filterDocument(ctx, n.Type().Name(), id, f)
	}
	for key, val := range f.(map[string]any) {
		switch key {
		case equalFilter:
			match, err := filterEqual(n, val)
			if err != nil || !match {
				return false, err
			}
		case notEqualFilter:
			match, err := filterEqual(n, val)
			if err != nil || match {
				return false, err
			}
		case greaterFilter:
			match, err := filterCompare(n, val)
			if err != nil || match <= 0 {
				return false, err
			}
		case greaterOrEqualFilter:
			match, err := filterCompare(n, val)
			if err != nil || match < 0 {
				return false, err
			}
		case lessFilter:
			match, err := filterCompare(n, val)
			if err != nil || match >= 0 {
				return false, err
			}
		case lessOrEqualFilter:
			match, err := filterCompare(n, val)
			if err != nil || match > 0 {
				return false, err
			}
		case inFilter:
			match, err := filterIn(n, val)
			if err != nil || !match {
				return false, err
			}
		case notInFilter:
			match, err := filterIn(n, val)
			if err != nil || match {
				return false, err
			}
		case andFilter:
			match, err := e.filterAnd(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case orFilter:
			match, err := e.filterOr(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case notFilter:
			match, err := e.filterNode(ctx, n, val)
			if err != nil || match {
				return false, err
			}
		case allFilter:
			match, err := e.filterAll(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case anyFilter:
			match, err := e.filterAny(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case noneFilter:
			match, err := e.filterAny(ctx, n, val)
			if err != nil || match {
				return false, err
			}
		default:
			field, err := n.LookupByString(key)
			if err != nil {
				return false, err
			}
			match, err := e.filterNode(ctx, field.(schema.TypedNode), val)
			if err != nil || !match {
				return false, err
			}
		}
	}
	return true, nil
}

func (e *executionContext) filterDocument(ctx context.Context, collection string, id string, f any) (bool, error) {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return false, err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return false, err
	}
	linkNode, err := collectionNode.LookupByString(id)
	if err != nil {
		return false, err
	}
	ctx = context.WithValue(ctx, idContextKey, id)
	return e.filterNode(ctx, linkNode.(schema.TypedNode), f)
}

func (e *executionContext) filterLink(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return false, err
	}
	obj, err := e.store.Load(ctx, lnk, node.Prototype(n))
	if err != nil {
		return false, err
	}
	ctx = context.WithValue(ctx, linkContextKey, lnk.String())
	return e.filterNode(ctx, obj.(schema.TypedNode), f)
}

func (e *executionContext) filterAll(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := e.filterNode(ctx, v.(schema.TypedNode), f)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *executionContext) filterAny(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := e.filterNode(ctx, v.(schema.TypedNode), f)
		if err != nil || match {
			return match, err
		}
	}
	return false, nil
}

func (e *executionContext) filterAnd(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	if f == nil {
		return true, nil
	}
	for _, v := range f.([]any) {
		match, err := e.filterNode(ctx, n, v.(map[string]any))
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *executionContext) filterOr(ctx context.Context, n schema.TypedNode, f any) (bool, error) {
	if f == nil {
		return true, nil
	}
	for _, v := range f.([]any) {
		match, err := e.filterNode(ctx, n, v.(map[string]any))
		if err != nil || match {
			return match, err
		}
	}
	return true, nil
}

func filterIn(n schema.TypedNode, value any) (bool, error) {
	switch n.Kind() {
	case datamodel.Kind_Int:
		v, err := n.AsInt()
		if err != nil {
			return false, err
		}
		return slices.Contains(value.([]int64), v), nil
	case datamodel.Kind_Float:
		v, err := n.AsFloat()
		if err != nil {
			return false, err
		}
		return slices.Contains(value.([]float64), v), nil
	case datamodel.Kind_String:
		v, err := n.AsString()
		if err != nil {
			return false, err
		}
		return slices.Contains(value.([]string), v), nil
	default:
		return false, fmt.Errorf("invalid kind for in filter: %s", n.Kind())
	}
}

func filterCompare(n schema.TypedNode, value any) (int, error) {
	switch n.Kind() {
	case datamodel.Kind_Int:
		v, err := n.AsInt()
		if err != nil {
			return 0, err
		}
		return cmp.Compare(v, value.(int64)), nil
	case datamodel.Kind_Float:
		v, err := n.AsFloat()
		if err != nil {
			return 0, err
		}
		return cmp.Compare(v, value.(float64)), nil
	case datamodel.Kind_String:
		v, err := n.AsString()
		if err != nil {
			return 0, err
		}
		return cmp.Compare(v, value.(string)), nil
	default:
		return 0, fmt.Errorf("invalid kind for compare filter: %s", n.Kind())
	}
}

func filterEqual(n schema.TypedNode, value any) (bool, error) {
	switch n.Kind() {
	case datamodel.Kind_Bool:
		v, err := n.AsBool()
		if err != nil {
			return false, err
		}
		return v == value, nil
	default:
		match, err := filterCompare(n, value)
		if err != nil {
			return false, err
		}
		return match == 0, nil
	}
}
