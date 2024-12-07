package graphql

import (
	"context"

	"github.com/nasdf/capy/core"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type contextKey string

var idContextKey = contextKey("id")

const (
	createOperationPrefix = "create"
	updateOperationPrefix = "update"
	deleteOperationPrefix = "delete"
	listOperationPrefix   = "list"
	findOperationPrefix   = "find"
)

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// Execute runs the query and returns a node containing the result of the query operation.
func Execute(ctx context.Context, store *core.Transaction, schema *ast.Schema, params QueryParams) (datamodel.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	ma, err := nb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	err = assignResults(ctx, store, schema, params, ma)
	if err != nil {
		return nil, err
	}
	err = ma.Finish()
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func assignResults(ctx context.Context, store *core.Transaction, schema *ast.Schema, params QueryParams, na datamodel.MapAssembler) error {
	query, errs := gqlparser.LoadQuery(schema, params.Query)
	if errs != nil {
		return assignErrors(errs, na)
	}
	var operation *ast.OperationDefinition
	if params.OperationName != "" {
		operation = query.Operations.ForName(params.OperationName)
	} else if len(query.Operations) == 1 {
		operation = query.Operations[0]
	}
	if operation == nil {
		return assignErrors(gqlerror.List{gqlerror.Errorf("operation is not defined")}, na)
	}
	exe := executionContext{
		store:  store,
		schema: schema,
		params: params,
		query:  query,
	}
	data, err := exe.execute(ctx, operation)
	if err != nil {
		return assignErrors(gqlerror.List{gqlerror.WrapIfUnwrapped(err)}, na)
	}
	va, err := na.AssembleEntry("data")
	if err != nil {
		return err
	}
	return va.AssignNode(data)
}

type executionContext struct {
	store  *core.Transaction
	schema *ast.Schema
	query  *ast.QueryDocument
	params QueryParams
}

func (e *executionContext) execute(ctx context.Context, operation *ast.OperationDefinition) (datamodel.Node, error) {
	res := basicnode.Prototype.Map.NewBuilder()
	switch operation.Operation {
	case ast.Mutation:
		err := e.executeMutation(ctx, operation.SelectionSet, res)
		if err != nil {
			return nil, err
		}

	case ast.Query:
		err := e.executeQuery(ctx, operation.SelectionSet, res)
		if err != nil {
			return nil, err
		}

	default:
		return nil, gqlerror.Errorf("unsupported operation %s", operation.Operation)
	}
	return res.Build(), nil
}

func (e *executionContext) collectFields(sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  e.params.Query,
		Variables: e.params.Variables,
		Doc:       e.query,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
