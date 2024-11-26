package graphql

import (
	"context"
	"fmt"
	"strings"

	"github.com/nasdf/capy/core"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeQuery(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(set, "Query")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		va, err := ma.AssembleEntry(field.Alias)
		if err != nil {
			return err
		}
		switch {
		case field.Name == "__typename":
			err = va.AssignString("Query")
			if err != nil {
				return err
			}

		case field.Name == "__type":
			err = e.introspectQueryType(field, va)
			if err != nil {
				return err
			}

		case field.Name == "__schema":
			err = e.introspectQuerySchema(field, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, findOperationPrefix):
			collection := strings.TrimPrefix(field.Name, findOperationPrefix)
			args := field.ArgumentMap(e.params.Variables)
			err = e.queryDocument(ctx, field, collection, args["id"].(string), va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, listOperationPrefix):
			collection := strings.TrimPrefix(field.Name, listOperationPrefix)
			err = e.queryCollection(ctx, field, collection, va)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("operation not supported %s", field.Name)
		}
	}
	return ma.Finish()
}

func (e *executionContext) queryDocument(ctx context.Context, field graphql.CollectedField, collection string, id string, na datamodel.NodeAssembler) error {
	rootNode, err := e.store.Load(ctx, e.rootLink, e.store.Prototype(core.RootTypeName))
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
	ctx = context.WithValue(ctx, idContextKey, id)
	return e.queryNode(ctx, linkNode.(schema.TypedNode), field, na)
}

func (e *executionContext) queryCollection(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	rootNode, err := e.store.Load(ctx, e.rootLink, e.store.Prototype(core.RootTypeName))
	if err != nil {
		return err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return err
	}
	la, err := na.BeginList(collectionNode.Length())
	if err != nil {
		return err
	}
	args := field.ArgumentMap(e.params.Variables)
	iter := collectionNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		val := v.(schema.TypedNode)
		key, err := k.AsString()
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, key)
		match, err := e.filterDocument(ctx, val, args["filter"])
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		err = e.queryNode(ctx, val, field, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) queryNode(ctx context.Context, n schema.TypedNode, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	if len(field.SelectionSet) == 0 {
		return na.AssignNode(n)
	}
	if e.store.IsRelation(n.Type()) {
		id, err := n.AsString()
		if err != nil {
			return err
		}
		return e.queryDocument(ctx, field, n.Type().Name(), id, na)
	}
	switch n.Kind() {
	case datamodel.Kind_Link:
		return e.queryLink(ctx, n, field, na)
	case datamodel.Kind_List:
		return e.queryList(ctx, n, field, na)
	case datamodel.Kind_Map:
		return e.queryMap(ctx, n, field, na)
	case datamodel.Kind_Null:
		return na.AssignNull()
	default:
		return gqlerror.ErrorPosf(field.Position, "cannot traverse node of type %s", n.Kind().String())
	}
}

func (e *executionContext) queryLink(ctx context.Context, n schema.TypedNode, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	lnk, err := n.AsLink()
	if err != nil {
		return err
	}
	obj, err := e.store.Load(ctx, lnk, core.Prototype(n))
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, linkContextKey, lnk.String())
	return e.queryNode(ctx, obj.(schema.TypedNode), field, na)
}

func (e *executionContext) queryList(ctx context.Context, n schema.TypedNode, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(n.Length())
	if err != nil {
		return err
	}
	iter := n.ListIterator()
	for !iter.Done() {
		_, obj, err := iter.Next()
		if err != nil {
			return err
		}
		err = e.queryNode(ctx, obj.(schema.TypedNode), field, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) queryMap(ctx context.Context, n schema.TypedNode, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	typeName := strings.TrimSuffix(n.Type().Name(), core.DocumentSuffix)
	fields := e.collectFields(field.SelectionSet, typeName)
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		va, err := ma.AssembleEntry(field.Alias)
		if err != nil {
			return err
		}
		switch field.Name {
		case "_link":
			err = va.AssignString(ctx.Value(linkContextKey).(string))
			if err != nil {
				return err
			}

		case "__typename":
			err = va.AssignString(typeName)
			if err != nil {
				return err
			}

		case "_id":
			err = va.AssignString(ctx.Value(idContextKey).(string))
			if err != nil {
				return err
			}

		default:
			obj, err := n.LookupByString(field.Name)
			if err != nil {
				return err
			}
			err = e.queryNode(ctx, obj.(schema.TypedNode), field, va)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}
