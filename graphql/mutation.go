package graphql

import (
	"context"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
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
	id, err := e.createDocument(ctx, collection, args["data"])
	if err != nil {
		return err
	}
	return e.queryDocument(ctx, field, collection, id, na)
}

func (e *executionContext) updateMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	var ids []string
	args := field.ArgumentMap(e.params.Variables)
	err := e.tx.ForEachDocument(ctx, collection, func(id string, doc datamodel.Node) error {
		ctx = context.WithValue(ctx, idContextKey, id)
		match, err := e.filterDocument(ctx, doc.(schema.TypedNode), args["filter"])
		if err != nil || !match {
			return err
		}
		ids = append(ids, id)
		return e.patchDocument(ctx, collection, id, doc.(schema.TypedNode), args["patch"])
	})
	if err != nil {
		return err
	}
	return e.queryDocuments(ctx, field, collection, ids, na)
}

func (e *executionContext) deleteMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(0)
	if err != nil {
		return err
	}
	args := field.ArgumentMap(e.params.Variables)
	err = e.tx.ForEachDocument(ctx, collection, func(id string, doc datamodel.Node) error {
		ctx = context.WithValue(ctx, idContextKey, id)
		match, err := e.filterDocument(ctx, doc.(schema.TypedNode), args["filter"])
		if err != nil || !match {
			return err
		}
		err = e.queryNode(ctx, doc.(schema.TypedNode), field, la.AssembleValue())
		if err != nil {
			return err
		}
		return e.tx.DeleteDocument(ctx, collection, id)
	})
	if err != nil {
		return err
	}
	return la.Finish()
}
