package graphql

import (
	"context"
	"strings"

	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeQuery(ctx context.Context, set ast.SelectionSet) (map[string]any, error) {
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
			res, err := e.queryCollection(ctx, field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *executionContext) queryDocument(ctx context.Context, field graphql.CollectedField, collection string, id string) (any, error) {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	linkNode, err := collectionNode.LookupByString(id)
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	ctx = context.WithValue(ctx, idContextKey, id)
	return e.queryField(ctx, linkNode.(schema.TypedNode), field)
}

func (e *executionContext) queryCollection(ctx context.Context, field graphql.CollectedField) (any, error) {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	collection, err := rootNode.LookupByString(field.Name)
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	result := make([]any, 0, collection.Length())
	iter := collection.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, gqlerror.ErrorPosf(field.Position, err.Error())
		}
		id, err := k.AsString()
		if err != nil {
			return nil, gqlerror.ErrorPosf(field.Position, err.Error())
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		out, err := e.queryField(ctx, v.(schema.TypedNode), field)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func (e *executionContext) queryField(ctx context.Context, n schema.TypedNode, field graphql.CollectedField) (any, error) {
	switch {
	case len(field.SelectionSet) == 0:
		return node.Value(n)
	case e.system.IsRelation(n.Type()):
		id, err := n.AsString()
		if err != nil {
			return nil, err
		}
		return e.queryDocument(ctx, field, n.Type().Name(), id)
	case n.Kind() == datamodel.Kind_Link:
		return e.queryLink(ctx, n, field)
	case n.Kind() == datamodel.Kind_List:
		return e.queryList(ctx, n, field)
	case n.Kind() == datamodel.Kind_Map:
		return e.queryMap(ctx, n, field)
	case n.Kind() == datamodel.Kind_Null:
		return nil, nil
	default:
		return nil, gqlerror.ErrorPosf(field.Position, "cannot traverse node of type %s", n.Kind().String())
	}
}

func (e *executionContext) queryLink(ctx context.Context, n schema.TypedNode, field graphql.CollectedField) (any, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	obj, err := e.store.Load(ctx, lnk, node.Prototype(n))
	if err != nil {
		return nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	ctx = context.WithValue(ctx, linkContextKey, lnk.String())
	return e.queryField(ctx, obj.(schema.TypedNode), field)
}

func (e *executionContext) queryList(ctx context.Context, n schema.TypedNode, field graphql.CollectedField) ([]any, error) {
	result := make([]any, 0, n.Length())
	iter := n.ListIterator()
	for !iter.Done() {
		_, obj, err := iter.Next()
		if err != nil {
			return nil, gqlerror.ErrorPosf(field.Position, err.Error())
		}
		val, err := e.queryField(ctx, obj.(schema.TypedNode), field)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func (e *executionContext) queryMap(ctx context.Context, n schema.TypedNode, field graphql.CollectedField) (any, error) {
	result := make(map[string]any)
	fields := e.collectFields(field.SelectionSet)
	for _, field := range fields {
		switch field.Name {
		case "_link":
			result[field.Alias] = ctx.Value(linkContextKey).(string)
		case "__typename":
			result[field.Alias] = strings.TrimSuffix(n.Type().Name(), types.DocumentSuffix)
		case "_id":
			result[field.Alias] = ctx.Value(idContextKey).(string)
		default:
			obj, err := n.LookupByString(field.Name)
			if err != nil {
				return nil, gqlerror.ErrorPosf(field.Position, err.Error())
			}
			val, err := e.queryField(ctx, obj.(schema.TypedNode), field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = val
		}
	}
	return result, nil
}
