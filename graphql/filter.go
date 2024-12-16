package graphql

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/vektah/gqlparser/v2/ast"
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

func (e *Request) filterDocument(ctx context.Context, collection string, doc map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for key, val := range filter.(map[string]any) {
		switch key {
		case andFilter:
			match, err := e.filterAnd(ctx, collection, doc, val)
			if err != nil || !match {
				return false, err
			}
		case orFilter:
			match, err := e.filterOr(ctx, collection, doc, val)
			if err != nil || !match {
				return false, err
			}
		case notFilter:
			match, err := e.filterDocument(ctx, collection, doc, val)
			if err != nil || match {
				return false, err
			}
		default:
			def, ok := e.schema.Types[collection]
			if !ok {
				return false, fmt.Errorf("invalid document type %s", collection)
			}
			field := def.Fields.ForName(key)
			if field == nil {
				return false, fmt.Errorf("invalid document field %s", key)
			}
			match, err := e.filterValue(ctx, field.Type, doc[key], val)
			if err != nil || !match {
				return false, err
			}
		}
	}
	return true, nil
}

func (e *Request) filterValue(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	def := e.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return e.filterRelation(ctx, typ, value, filter.(map[string]any))
	}
	for key, val := range filter.(map[string]any) {
		switch key {
		case equalFilter:
			match, err := filterEqual(value, val)
			if err != nil || !match {
				return false, err
			}
		case notEqualFilter:
			match, err := filterEqual(value, val)
			if err != nil || match {
				return false, err
			}
		case greaterFilter:
			match, err := filterCompare(value, val)
			if err != nil || match <= 0 {
				return false, err
			}
		case greaterOrEqualFilter:
			match, err := filterCompare(value, val)
			if err != nil || match < 0 {
				return false, err
			}
		case lessFilter:
			match, err := filterCompare(value, val)
			if err != nil || match >= 0 {
				return false, err
			}
		case lessOrEqualFilter:
			match, err := filterCompare(value, val)
			if err != nil || match > 0 {
				return false, err
			}
		case inFilter:
			match, err := filterIn(value, val)
			if err != nil || !match {
				return false, err
			}
		case notInFilter:
			match, err := filterIn(value, val)
			if err != nil || match {
				return false, err
			}
		case allFilter:
			match, err := e.filterAll(ctx, typ, value, val)
			if err != nil || !match {
				return false, err
			}
		case anyFilter:
			match, err := e.filterAny(ctx, typ, value, val)
			if err != nil || !match {
				return false, err
			}
		case noneFilter:
			match, err := e.filterAny(ctx, typ, value, val)
			if err != nil || match {
				return false, err
			}
		default:
			return false, fmt.Errorf("invalid filter operator %s", key)
		}
	}
	return true, nil
}

func (e *Request) filterRelation(ctx context.Context, typ *ast.Type, value any, filter map[string]any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	doc, err := e.tx.ReadDocument(ctx, typ.NamedType, value.(string))
	if err != nil {
		return false, err
	}
	return e.filterDocument(ctx, typ.NamedType, doc, filter)
}

func (e *Request) filterAnd(ctx context.Context, collection string, value map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range filter.([]any) {
		match, err := e.filterDocument(ctx, collection, value, v)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *Request) filterOr(ctx context.Context, collection string, value map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range filter.([]any) {
		match, err := e.filterDocument(ctx, collection, value, v)
		if err != nil || match {
			return match, err
		}
	}
	return true, nil
}

func (e *Request) filterAll(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := e.filterValue(ctx, typ.Elem, v, filter)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (e *Request) filterAny(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := e.filterValue(ctx, typ.Elem, v, filter)
		if err != nil || match {
			return match, err
		}
	}
	return false, nil
}

func filterIn(value any, filter any) (bool, error) {
	switch v := value.(type) {
	case int64:
		return slices.Contains(filter.([]int64), v), nil
	case float64:
		return slices.Contains(filter.([]float64), v), nil
	case string:
		return slices.Contains(filter.([]string), v), nil
	default:
		return false, fmt.Errorf("invalid kind for in filter")
	}
}

func filterCompare(value any, filter any) (int, error) {
	switch v := value.(type) {
	case int64:
		return cmp.Compare(v, filter.(int64)), nil
	case float64:
		return cmp.Compare(v, filter.(float64)), nil
	case string:
		return cmp.Compare(v, filter.(string)), nil
	default:
		return 0, fmt.Errorf("invalid kind for compare filter")
	}
}

func filterEqual(value any, filter any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v == filter, nil
	default:
		match, err := filterCompare(v, filter)
		if err != nil {
			return false, err
		}
		return match == 0, nil
	}
}
