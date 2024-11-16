package graphql

import (
	"context"
	"strings"

	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func (e *executionContext) executeMutation(ctx context.Context, rootLink datamodel.Link, set ast.SelectionSet) (map[string]any, error) {
	fields := e.collectFields(set, "Mutation")
	out := make(map[string]any)
	for _, field := range fields {
		switch {
		case strings.HasPrefix(field.Name, "create"):
			val, lnk, err := e.createMutation(ctx, rootLink, field)
			if err != nil {
				return nil, err
			}
			rootLink = lnk
			out[field.Alias] = val

		default:
			return nil, gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	err := e.store.SetRootLink(ctx, rootLink)
	if err != nil {
		return nil, gqlerror.Wrap(err)
	}
	return out, nil
}

func (e *executionContext) createMutation(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField) (any, datamodel.Link, error) {
	args := field.ArgumentMap(e.params.Variables)
	collection := strings.TrimPrefix(field.Name, "create")
	builder := node.NewBuilder(e.store, e.system)

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
