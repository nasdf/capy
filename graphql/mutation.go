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

func (e *executionContext) executeMutation(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
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
		case strings.HasPrefix(field.Name, createMutationPrefix):
			collection := strings.TrimPrefix(field.Name, createMutationPrefix)
			lnk, err := e.createMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}
			rootLink = lnk

		default:
			return gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	err = e.store.SetRootLink(ctx, rootLink)
	if err != nil {
		return err
	}
	return ma.Finish()
}

func (e *executionContext) createMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) (datamodel.Link, error) {
	args := field.ArgumentMap(e.params.Variables)
	builder := node.NewBuilder(e.store, e.system)
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)

	id, err := builder.Build(ctx, collection, args["data"])
	if err != nil {
		return nil, err
	}
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, err
	}

	for k, v := range builder.Documents() {
		rootNode, err = e.store.SetNode(ctx, datamodel.ParsePath(k), rootNode, basicnode.NewLink(v))
		if err != nil {
			return nil, err
		}
	}

	rootPath := datamodel.ParsePath(types.RootParentsFieldName).AppendSegmentString("-")
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(rootLink))
	if err != nil {
		return nil, err
	}
	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, rootContextKey, rootLink)
	err = e.queryDocument(ctx, field, collection, id, na)
	if err != nil {
		return nil, err
	}
	return rootLink, nil
}
