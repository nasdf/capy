package graphql

import (
	"context"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeMutation(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(set, "Mutation")
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
		case strings.HasPrefix(field.Name, createOperationPrefix):
			collection := strings.TrimPrefix(field.Name, createOperationPrefix)
			err = e.createMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, updateOperationPrefix):
			collection := strings.TrimPrefix(field.Name, updateOperationPrefix)
			err = e.updateMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, deleteOperationPrefix):
			collection := strings.TrimPrefix(field.Name, deleteOperationPrefix)
			err = e.deleteMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		default:
			return gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	return ma.Finish()
}

func (e *executionContext) createMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	data, _ := args["data"].(map[string]any)
	id, err := e.store.CreateDocument(ctx, collection, data)
	if err != nil {
		return err
	}
	return e.findQuery(ctx, field, collection, id, na)
}

func (e *executionContext) updateMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	filter, _ := args["filter"].(map[string]any)
	patch, _ := args["patch"].(map[string]any)

	iter, err := e.store.DocumentIterator(ctx, collection)
	if err != nil {
		return err
	}

	var ids []string
	for !iter.Done() {
		id, doc, err := iter.Next(ctx)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		match, err := e.filterDocument(ctx, collection, doc, filter)
		if err != nil || !match {
			return err
		}
		err = e.store.PatchDocument(ctx, collection, id, patch)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}
	la, err := na.BeginList(0)
	if err != nil {
		return err
	}
	for _, id := range ids {
		doc, err := e.store.ReadDocument(ctx, collection, id)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		err = e.queryDocument(ctx, collection, doc, field, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) deleteMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	filter, _ := args["filter"].(map[string]any)

	la, err := na.BeginList(0)
	if err != nil {
		return err
	}
	iter, err := e.store.DocumentIterator(ctx, collection)
	if err != nil {
		return err
	}
	for !iter.Done() {
		id, doc, err := iter.Next(ctx)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, id)
		match, err := e.filterDocument(ctx, collection, doc, filter)
		if err != nil || !match {
			return err
		}
		err = e.queryDocument(ctx, collection, doc, field, la.AssembleValue())
		if err != nil {
			return err
		}
		err = e.store.DeleteDocument(ctx, collection, id)
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
