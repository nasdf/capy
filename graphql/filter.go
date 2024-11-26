package graphql

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/nasdf/capy/core"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
)

const (
	equalFilter          = "eq"
	notEqualFilter       = "neq"
	greaterFilter        = "gt"
	greaterOrEqualFilter = "gte"
	lessFilter           = "lt"
	lessOrEqualFilter    = "lte"
	inFilter             = "in"
	notInFilter          = "nin"
	andFilter            = "and"
	orFilter             = "or"
	notFilter            = "not"
	allFilter            = "all"
	anyFilter            = "any"
	noneFilter           = "none"
)

func (e *executionContext) filterDocument(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	if n.Kind() == datamodel.Kind_Link {
		return e.filterLink(ctx, n, value)
	}
	for key, val := range value.(map[string]any) {
		switch key {
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
			match, err := e.filterDocument(ctx, n, val.(map[string]any))
			if err != nil || match {
				return false, err
			}
		default:
			field, err := n.LookupByString(key)
			if err != nil {
				return false, err
			}
			match, err := e.filterField(ctx, field.(schema.TypedNode), val)
			if err != nil || !match {
				return false, err
			}
		}
	}
	return true, nil
}

func (e *executionContext) filterField(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	if e.store.IsRelation(n.Type()) {
		id, err := n.AsString()
		if err != nil {
			return false, err
		}
		return e.filterRelation(ctx, n.Type().Name(), id, value.(map[string]any))
	}
	for key, val := range value.(map[string]any) {
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
			return false, fmt.Errorf("invalid filter operator %s", key)
		}
	}
	return true, nil
}

func (e *executionContext) filterLink(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	lnk, err := n.AsLink()
	if err != nil {
		return false, err
	}
	obj, err := e.store.Load(ctx, lnk, core.Prototype(n))
	if err != nil {
		return false, err
	}
	return e.filterDocument(ctx, obj.(schema.TypedNode), value)
}

func (e *executionContext) filterRelation(ctx context.Context, collection, id string, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	rootNode, err := e.store.RootNode(ctx)
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
	return e.filterLink(ctx, linkNode.(schema.TypedNode), value)
}

func (e *executionContext) filterAnd(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := e.filterDocument(ctx, n, v.(map[string]any))
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *executionContext) filterOr(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := e.filterDocument(ctx, n, v.(map[string]any))
		if err != nil || match {
			return match, err
		}
	}
	return true, nil
}

func (e *executionContext) filterAll(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := e.filterField(ctx, v.(schema.TypedNode), value)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *executionContext) filterAny(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := e.filterField(ctx, v.(schema.TypedNode), value)
		if err != nil || match {
			return match, err
		}
	}
	return false, nil
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
