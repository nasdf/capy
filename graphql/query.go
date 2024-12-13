package graphql

import (
	"context"
	"fmt"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *Context) executeQuery(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
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
			err = e.findQuery(ctx, field, collection, args["id"].(string), va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, listOperationPrefix):
			collection := strings.TrimPrefix(field.Name, listOperationPrefix)
			err = e.listQuery(ctx, field, collection, va)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("operation not supported %s", field.Name)
		}
	}
	return ma.Finish()
}

func (e *Context) findQuery(ctx context.Context, field graphql.CollectedField, collection string, id string, na datamodel.NodeAssembler) error {
	doc, err := e.collections.ReadDocument(ctx, collection, id)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, idContextKey, id)
	return e.queryDocument(ctx, collection, doc, field, na)
}

func (e *Context) listQuery(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(0)
	if err != nil {
		return err
	}
	iter, err := e.collections.DocumentIterator(ctx, collection)
	if err != nil {
		return err
	}
	args := field.ArgumentMap(e.params.Variables)
	for !iter.Done() {
		id, doc, err := iter.Next(ctx)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		match, err := e.filterDocument(ctx, collection, doc, args["filter"])
		if err != nil || !match {
			return err
		}
		err = e.queryDocument(ctx, collection, doc, field, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *Context) queryDocument(ctx context.Context, collection string, n datamodel.Node, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	fields := e.collectFields(field.SelectionSet, collection)
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, f := range fields {
		va, err := ma.AssembleEntry(f.Alias)
		if err != nil {
			return err
		}
		switch f.Name {
		case "__typename":
			err = va.AssignString(collection)
			if err != nil {
				return err
			}

		case "_id":
			err = va.AssignString(ctx.Value(idContextKey).(string))
			if err != nil {
				return err
			}

		default:
			def, ok := e.schema.Types[collection]
			if !ok {
				return fmt.Errorf("invalid document type %s", collection)
			}
			field := def.Fields.ForName(f.Name)
			if field == nil {
				return fmt.Errorf("invalid document field %s", f.Name)
			}
			fn, err := n.LookupByString(field.Name)
			if _, ok := err.(datamodel.ErrNotExists); !ok && err != nil {
				return err
			}
			if fn == nil || fn.IsNull() || fn.IsAbsent() {
				err = va.AssignNull()
			} else {
				err = e.queryNode(ctx, field.Type, fn, f, va)
			}
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *Context) queryNode(ctx context.Context, typ *ast.Type, n datamodel.Node, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	if len(field.SelectionSet) == 0 {
		return na.AssignNode(n)
	}
	if typ.Elem != nil {
		return e.queryList(ctx, typ, n, field, na)
	}
	def := e.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return e.queryRelation(ctx, typ, n, field, na)
	}
	return gqlerror.ErrorPosf(field.Position, "cannot traverse node of type %s", n.Kind().String())
}

func (e *Context) queryRelation(ctx context.Context, typ *ast.Type, n datamodel.Node, field graphql.CollectedField, na datamodel.NodeAssembler) error {
	id, err := n.AsString()
	if err != nil {
		return err
	}
	doc, err := e.collections.ReadDocument(ctx, typ.NamedType, id)
	if err != nil {
		return err
	}
	return e.queryDocument(ctx, typ.NamedType, doc, field, na)
}

func (e *Context) queryList(ctx context.Context, typ *ast.Type, n datamodel.Node, field graphql.CollectedField, na datamodel.NodeAssembler) error {
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
		err = e.queryNode(ctx, typ.Elem, obj, field, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
