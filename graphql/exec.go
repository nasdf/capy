package graphql

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type contextKey string

var linkContextKey = contextKey("link")

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
	var operation *ast.OperationDefinition
	if params.OperationName != "" {
		operation = doc.Operations.ForName(params.OperationName)
	} else if len(doc.Operations) == 1 {
		operation = doc.Operations[0]
	}
	if operation == nil {
		return nil, gqlerror.List{gqlerror.Errorf("operation is not defined")}
	}
	rootLink, err := store.RootLink(ctx)
	if err != nil {
		return nil, gqlerror.List{gqlerror.Wrap(err)}
	}
	exec := executionContext{
		schema:   schema,
		store:    store,
		system:   system,
		queryDoc: doc,
		params:   params,
		rootLink: rootLink,
	}
	switch operation.Operation {
	case ast.Mutation:
		return exec.executeMutation(ctx, rootLink, operation.SelectionSet)
	case ast.Query:
		return exec.executeQuery(ctx, rootLink, operation.SelectionSet)
	default:
		return nil, gqlerror.List{gqlerror.Errorf("unsupported operation %s", operation.Operation)}
	}
}

type executionContext struct {
	schema   *ast.Schema
	store    *core.Store
	system   *types.System
	queryDoc *ast.QueryDocument
	params   QueryParams
	rootLink datamodel.Link
}

func (e *executionContext) collectFields(sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  e.params.Query,
		Variables: e.params.Variables,
		Doc:       e.queryDoc,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
