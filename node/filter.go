package node

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/types"

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

// Filter is a set of operators used to match document fields.
type Filter struct {
	store  *core.Store
	system *types.System
	value  any
}

func NewFilter(store *core.Store, system *types.System, value any) *Filter {
	return &Filter{
		store:  store,
		system: system,
		value:  value,
	}
}

func (f *Filter) Match(ctx context.Context, n schema.TypedNode) (bool, error) {
	return f.matchDocument(ctx, n, f.value)
}

func (f *Filter) matchDocument(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	if n.Kind() == datamodel.Kind_Link {
		return f.matchLink(ctx, n, value)
	}
	for key, val := range value.(map[string]any) {
		switch key {
		case andFilter:
			match, err := f.matchAnd(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case orFilter:
			match, err := f.matchOr(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case notFilter:
			match, err := f.matchDocument(ctx, n, val.(map[string]any))
			if err != nil || match {
				return false, err
			}
		default:
			field, err := n.LookupByString(key)
			if err != nil {
				return false, err
			}
			match, err := f.matchField(ctx, field.(schema.TypedNode), val)
			if err != nil || !match {
				return false, err
			}
		}
	}
	return true, nil
}

func (f *Filter) matchField(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	if f.system.IsRelation(n.Type()) {
		id, err := n.AsString()
		if err != nil {
			return false, err
		}
		return f.matchRelation(ctx, n.Type().Name(), id, value.(map[string]any))
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
			match, err := f.matchAll(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case anyFilter:
			match, err := f.matchAny(ctx, n, val)
			if err != nil || !match {
				return false, err
			}
		case noneFilter:
			match, err := f.matchAny(ctx, n, val)
			if err != nil || match {
				return false, err
			}
		default:
			return false, fmt.Errorf("invalid filter operator %s", key)
		}
	}
	return true, nil
}

func (f *Filter) matchLink(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	lnk, err := n.AsLink()
	if err != nil {
		return false, err
	}
	obj, err := f.store.Load(ctx, lnk, Prototype(n))
	if err != nil {
		return false, err
	}
	return f.matchDocument(ctx, obj.(schema.TypedNode), value)
}

func (f *Filter) matchRelation(ctx context.Context, collection, id string, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	rootLink, err := f.store.RootLink(ctx)
	if err != nil {
		return false, err
	}
	rootNode, err := f.store.Load(ctx, rootLink, f.system.Prototype(types.RootTypeName))
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
	return f.matchLink(ctx, linkNode.(schema.TypedNode), value)
}

func (f *Filter) matchAnd(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := f.matchDocument(ctx, n, v.(map[string]any))
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (f *Filter) matchOr(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := f.matchDocument(ctx, n, v.(map[string]any))
		if err != nil || match {
			return match, err
		}
	}
	return true, nil
}

func (f *Filter) matchAll(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := f.matchField(ctx, v.(schema.TypedNode), value)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (f *Filter) matchAny(ctx context.Context, n schema.TypedNode, value any) (bool, error) {
	if value == nil {
		return true, nil
	}
	iter := n.ListIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return false, err
		}
		match, err := f.matchField(ctx, v.(schema.TypedNode), value)
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
