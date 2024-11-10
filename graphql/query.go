package graphql

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeQuery(ctx context.Context, rootLink datamodel.Link, set ast.SelectionSet) (map[string]any, error) {
	fields := e.collectFields(set, "Query")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "Query"
		case "__type":
			result[field.Alias] = e.introspectQueryType(field)
		case "__schema":
			result[field.Alias] = e.introspectQuerySchema(field)
		default:
			res, err := e.queryCollection(ctx, rootLink, field)
			if err != nil {
				return nil, gqlerror.List{gqlerror.Wrap(err)}
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *executionContext) queryColletionByID(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField, id string) (any, error) {
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, err
	}
	collection, err := rootNode.LookupByString(field.Name)
	if err != nil {
		return nil, err
	}
	obj, err := collection.LookupByString(id)
	if err != nil {
		return nil, err
	}
	return e.queryField(ctx, obj, field)
}

func (e *executionContext) queryCollection(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField) (any, error) {
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, err
	}
	collection, err := rootNode.LookupByString(field.Name)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, collection.Length())
	iter := collection.MapIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		out, err := e.queryField(ctx, v, field)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func (e *executionContext) queryField(ctx context.Context, n datamodel.Node, field graphql.CollectedField) (any, error) {
	if len(field.SelectionSet) == 0 {
		return node.Value(n)
	}
	switch n.Kind() {
	case datamodel.Kind_Link:
		return e.queryLink(ctx, n, field)
	case datamodel.Kind_List:
		return e.queryList(ctx, n, field)
	case datamodel.Kind_Map:
		return e.queryMap(ctx, n, field.SelectionSet)
	case datamodel.Kind_Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot traverse node of type %s", n.Kind().String())
	}
}

func (e *executionContext) queryLink(ctx context.Context, n datamodel.Node, field graphql.CollectedField) (any, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return nil, err
	}
	obj, err := e.store.Load(ctx, lnk, node.Prototype(n))
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, linkContextKey, lnk)
	return e.queryField(ctx, obj, field)
}

func (e *executionContext) queryList(ctx context.Context, n datamodel.Node, field graphql.CollectedField) ([]any, error) {
	result := make([]any, 0, n.Length())
	iter := n.ListIterator()
	for !iter.Done() {
		_, obj, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := e.queryField(ctx, obj, field)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func (e *executionContext) queryMap(ctx context.Context, n datamodel.Node, set ast.SelectionSet) (any, error) {
	result := make(map[string]any)
	fields := e.collectFields(set)
	for _, field := range fields {
		switch field.Name {
		case "_link":
			result[field.Alias] = ctx.Value(linkContextKey).(datamodel.Link).String()
		case "__typename":
			result[field.Alias] = n.(schema.TypedNode).Type().Name()
		default:
			obj, err := n.LookupByString(field.Name)
			if err != nil {
				return nil, err
			}
			val, err := e.queryField(ctx, obj, field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = val
		}
	}
	return result, nil
}
