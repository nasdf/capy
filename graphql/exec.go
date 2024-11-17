package graphql

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type contextKey string

var (
	idContextKey   = contextKey("id")
	rootContextKey = contextKey("root")
	linkContextKey = contextKey("link")
)

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// QueryResponse contains the fields expected from a GraphQL response.
type QueryResponse struct {
	Data   any `json:"data"`
	Errors any `json:"errors,omitempty"`
}

func Execute(ctx context.Context, system *types.System, store *core.Store, schema *ast.Schema, params QueryParams) (any, error) {
	doc, errs := gqlparser.LoadQuery(schema, params.Query)
	if errs != nil {
		return nil, errs
	}
	exe := executionContext{
		schema: schema,
		store:  store,
		system: system,
		params: params,
		query:  doc,
	}
	data, err := exe.execute(ctx)
	if err != nil {
		return data, gqlerror.List{gqlerror.WrapIfUnwrapped(err)}
	}
	return data, nil
}

type executionContext struct {
	schema *ast.Schema
	store  *core.Store
	system *types.System
	query  *ast.QueryDocument
	params QueryParams
}

func (e *executionContext) execute(ctx context.Context) (any, error) {
	var operation *ast.OperationDefinition
	if e.params.OperationName != "" {
		operation = e.query.Operations.ForName(e.params.OperationName)
	} else if len(e.query.Operations) == 1 {
		operation = e.query.Operations[0]
	}
	if operation == nil {
		return nil, gqlerror.Errorf("operation is not defined")
	}
	rootLink, err := e.store.RootLink(ctx)
	if err != nil {
		return nil, gqlerror.Wrap(err)
	}
	ctx = context.WithValue(ctx, rootContextKey, rootLink)
	switch operation.Operation {
	case ast.Mutation:
		return e.executeMutation(ctx, operation.SelectionSet)
	case ast.Query:
		return e.executeQuery(ctx, operation.SelectionSet)
	default:
		return nil, gqlerror.Errorf("unsupported operation %s", operation.Operation)
	}
}

func (e *executionContext) collectFields(sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  e.params.Query,
		Variables: e.params.Variables,
		Doc:       e.query,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
