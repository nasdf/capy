package graphql

import (
	"context"
	"slices"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *Request) executeMutation(ctx context.Context, set ast.SelectionSet) (any, error) {
	fields := e.collectFields(set, "Mutation")
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		switch {
		case strings.HasPrefix(field.Name, createOperationPrefix):
			collection := strings.TrimPrefix(field.Name, createOperationPrefix)
			res, err := e.createMutation(ctx, field, collection)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case strings.HasPrefix(field.Name, updateOperationPrefix):
			collection := strings.TrimPrefix(field.Name, updateOperationPrefix)
			res, err := e.updateMutation(ctx, field, collection)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		case strings.HasPrefix(field.Name, deleteOperationPrefix):
			collection := strings.TrimPrefix(field.Name, deleteOperationPrefix)
			res, err := e.deleteMutation(ctx, field, collection)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res

		default:
			return nil, gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	return result, nil
}

func (e *Request) createMutation(ctx context.Context, field graphql.CollectedField, collection string) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	data, _ := args["data"].(map[string]any)
	id, err := e.tx.CreateDocument(ctx, collection, data)
	if err != nil {
		return nil, err
	}
	return e.findQuery(ctx, field, collection, id)
}

func (e *Request) updateMutation(ctx context.Context, field graphql.CollectedField, collection string) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	filter, _ := args["filter"].(map[string]any)
	patch, _ := args["patch"].(map[string]any)

	iter, err := e.tx.DocumentIterator(ctx, collection)
	if err != nil {
		return nil, err
	}
	updates := make([]string, 0)
	for !iter.Done() {
		id, hash, _, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		ctx = context.WithValue(ctx, hashContextKey, hash.String())
		match, err := e.tx.FilterDocument(ctx, collection, id, filter)
		if err != nil || !match {
			return nil, err
		}
		err = e.tx.PatchDocument(ctx, collection, id, patch)
		if err != nil {
			return nil, err
		}
		updates = append(updates, id)
	}
	iter, err = e.tx.DocumentIterator(ctx, collection)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(updates))
	for !iter.Done() {
		id, hash, doc, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(updates, id) {
			continue
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		ctx = context.WithValue(ctx, hashContextKey, hash.String())
		data, err := e.queryDocument(ctx, collection, doc, field)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}

func (e *Request) deleteMutation(ctx context.Context, field graphql.CollectedField, collection string) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	filter, _ := args["filter"].(map[string]any)

	iter, err := e.tx.DocumentIterator(ctx, collection)
	if err != nil {
		return nil, err
	}
	var result []any
	for !iter.Done() {
		id, hash, doc, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		ctx = context.WithValue(ctx, hashContextKey, hash.String())
		match, err := e.tx.FilterDocument(ctx, collection, id, filter)
		if err != nil || !match {
			return nil, err
		}
		data, err := e.queryDocument(ctx, collection, doc, field)
		if err != nil {
			return nil, err
		}
		err = e.tx.DeleteDocument(ctx, collection, id)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}
