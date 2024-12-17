package graphql

import (
	"context"
	"fmt"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const (
	idFieldName      = "id"
	hashFieldName    = "hash"
	commitsFieldName = "commits"
)

func (e *Request) executeQuery(ctx context.Context, set ast.SelectionSet) (any, error) {
	fields := e.collectFields(set, "Query")
	result := make(map[string]any)
	for _, field := range fields {
		switch {
		case field.Name == "__typename":
			result[field.Alias] = "Query"

		case field.Name == "__type":
			res, err := e.introspectQueryType(field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case field.Name == "__schema":
			res, err := e.introspectQuerySchema(field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case field.Name == commitsFieldName:
			res, err := e.commitsQuery(ctx, field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case strings.HasPrefix(field.Name, findOperationPrefix):
			collection := strings.TrimPrefix(field.Name, findOperationPrefix)
			args := field.ArgumentMap(e.params.Variables)
			res, err := e.findQuery(ctx, field, collection, args["id"].(string))
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case strings.HasPrefix(field.Name, listOperationPrefix):
			collection := strings.TrimPrefix(field.Name, listOperationPrefix)
			res, err := e.listQuery(ctx, field, collection)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		default:
			return nil, fmt.Errorf("operation not supported %s", field.Name)
		}
	}
	return result, nil
}

func (e *Request) commitsQuery(ctx context.Context, field graphql.CollectedField) (any, error) {
	fields := e.collectFields(field.SelectionSet, commitsFieldName)
	result := make([]any, 0)
	iter := e.tx.CommitIterator()
	for !iter.Done() {
		l, _, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		res := make(map[string]any)
		for _, f := range fields {
			switch f.Name {
			case hashFieldName:
				res[f.Alias] = l.String()
			default:
				return nil, fmt.Errorf("unknown commit field: %s", f.Name)
			}
		}
		result = append(result, res)
	}
	return result, nil
}

func (e *Request) findQuery(ctx context.Context, field graphql.CollectedField, collection string, id string) (any, error) {
	doc, err := e.tx.ReadDocument(ctx, collection, id)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, idContextKey, id)
	return e.queryDocument(ctx, collection, doc, field)
}

func (e *Request) listQuery(ctx context.Context, field graphql.CollectedField, collection string) (any, error) {
	iter, err := e.tx.DocumentIterator(ctx, collection)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0)
	args := field.ArgumentMap(e.params.Variables)
	for !iter.Done() {
		id, hash, doc, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		ctx = context.WithValue(ctx, hashContextKey, hash.String())
		match, err := e.tx.FilterDocument(ctx, collection, id, args["filter"])
		if err != nil || !match {
			return nil, err
		}
		res, err := e.queryDocument(ctx, collection, doc, field)
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}

func (e *Request) queryDocument(ctx context.Context, collection string, doc map[string]any, field graphql.CollectedField) (any, error) {
	fields := e.collectFields(field.SelectionSet, collection)
	result := make(map[string]any)
	for _, f := range fields {
		switch f.Name {
		case "__typename":
			result[f.Alias] = collection

		case hashFieldName:
			result[f.Alias] = ctx.Value(hashContextKey).(string)

		case idFieldName:
			result[f.Alias] = ctx.Value(idContextKey).(string)

		default:
			def, ok := e.schema.Types[collection]
			if !ok {
				return nil, fmt.Errorf("invalid document type %s", collection)
			}
			fd := def.Fields.ForName(f.Name)
			if fd == nil {
				return nil, fmt.Errorf("invalid document field %s", f.Name)
			}
			res, err := e.queryValue(ctx, fd.Type, doc[f.Name], f)
			if err != nil {
				return nil, err
			}
			result[f.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) queryValue(ctx context.Context, typ *ast.Type, value any, field graphql.CollectedField) (any, error) {
	if value == nil || len(field.SelectionSet) == 0 {
		return value, nil
	}
	if typ.Elem != nil {
		return e.queryList(ctx, typ, value, field)
	}
	def := e.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return e.queryRelation(ctx, typ, value, field)
	}
	return nil, gqlerror.ErrorPosf(field.Position, "cannot traverse scalar type")
}

func (e *Request) queryRelation(ctx context.Context, typ *ast.Type, value any, field graphql.CollectedField) (any, error) {
	if value == nil {
		return nil, nil
	}
	doc, err := e.tx.ReadDocument(ctx, typ.NamedType, value.(string))
	if err != nil {
		return nil, err
	}
	return e.queryDocument(ctx, typ.NamedType, doc, field)
}

func (e *Request) queryList(ctx context.Context, typ *ast.Type, value any, field graphql.CollectedField) (any, error) {
	result := make([]any, 0)
	for _, v := range value.([]any) {
		res, err := e.queryValue(ctx, typ.Elem, v, field)
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}
