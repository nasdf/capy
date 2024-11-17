package graphql

import (
	"context"
	"strings"

	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const createMutationPrefix = "create"

func (e *executionContext) executeMutation(ctx context.Context, set ast.SelectionSet) (map[string]any, error) {
	fields := e.collectFields(set, "Mutation")
	result := make(map[string]any)
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)

	for _, field := range fields {
		switch {
		case strings.HasPrefix(field.Name, createMutationPrefix):
			collection := strings.TrimPrefix(field.Name, createMutationPrefix)
			val, lnk, err := e.createMutation(ctx, field, collection)
			if err != nil {
				return nil, err
			}
			rootLink = lnk
			result[field.Alias] = val

		default:
			return nil, gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}

	err := e.store.SetRootLink(ctx, rootLink)
	if err != nil {
		return nil, gqlerror.Wrap(err)
	}
	return result, nil
}

func (e *executionContext) createMutation(ctx context.Context, field graphql.CollectedField, collection string) (any, datamodel.Link, error) {
	args := field.ArgumentMap(e.params.Variables)
	builder := node.NewBuilder(e.store, e.system)
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)

	id, err := builder.Build(ctx, collection, args["data"])
	if err != nil {
		return nil, nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}

	for collection, documents := range builder.Links() {
		for id, lnk := range documents {
			rootPath := datamodel.ParsePath(collection).AppendSegmentString(id)
			rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(lnk))
			if err != nil {
				return nil, nil, gqlerror.ErrorPosf(field.Position, err.Error())
			}
		}
	}

	rootPath := datamodel.ParsePath(types.RootParentsFieldName).AppendSegmentString("-")
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(rootLink))
	if err != nil {
		return nil, nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}
	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, nil, gqlerror.ErrorPosf(field.Position, err.Error())
	}

	ctx = context.WithValue(ctx, rootContextKey, rootLink)
	val, err := e.queryDocument(ctx, field, collection, id)
	if err != nil {
		return nil, nil, err
	}
	return val, rootLink, nil
}
