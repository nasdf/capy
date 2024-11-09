package graphql

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
)

func (e *executionContext) executeMutation(ctx context.Context, rootLink datamodel.Link, set ast.SelectionSet) (map[string]any, error) {
	fields := e.collectFields(set, "Mutation")
	out := make(map[string]any)
	for _, field := range fields {
		switch {
		case strings.HasPrefix(field.Name, "create"):
			val, lnk, err := e.createMutation(ctx, rootLink, field)
			if err != nil {
				return nil, gqlerror.List{gqlerror.Wrap(err)}
			}
			rootLink = lnk
			out[field.Alias] = val

		default:
			return nil, gqlerror.List{gqlerror.Errorf("unsupported mutation %s", field.Name)}
		}
	}
	err := e.store.SetRootLink(ctx, rootLink)
	if err != nil {
		return nil, gqlerror.List{gqlerror.Wrap(err)}
	}
	return out, nil
}

func (e *executionContext) createMutation(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField) (any, datamodel.Link, error) {
	args := field.ArgumentMap(e.params.Variables)
	collection := strings.TrimPrefix(field.Name, "create")

	lnk, err := node.Build(ctx, e.store, e.system.Type(collection), args["input"])
	if err != nil {
		return nil, nil, err
	}
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, nil, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, err
	}

	path := datamodel.ParsePath(collection).AppendSegmentString(id.String())
	rootNode, err = e.store.Traversal(ctx).FocusedTransform(rootNode, path, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
		return basicnode.NewLink(lnk), nil
	}, true)
	if err != nil {
		return nil, nil, err
	}

	// set the field name so we query the correct collection
	field.Name = collection
	// set the span so we only query the newly created object
	ctx = context.WithValue(ctx, spanContextKey, int64(-1))

	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, nil, err
	}
	val, err := e.queryRoot(ctx, rootLink, field)
	if err != nil {
		return nil, nil, err
	}
	return val, rootLink, nil
}
