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

type executionContext struct {
	store  *core.Transaction
	query  *ast.QueryDocument
	params QueryParams
}

func createExecutionContext(store *core.Transaction, params QueryParams) (*executionContext, gqlerror.List) {
	query, errs := gqlparser.LoadQuery(store.Schema(), params.Query)
	if errs != nil {
		return nil, errs
	}
	return &executionContext{
		store:  store,
		params: params,
		query:  query,
	}, nil
}

func (e *executionContext) execute(ctx context.Context) (datamodel.Node, error) {
	var operation *ast.OperationDefinition
	if e.params.OperationName != "" {
		operation = e.query.Operations.ForName(e.params.OperationName)
	} else if len(e.query.Operations) == 1 {
		operation = e.query.Operations[0]
	}
	if operation == nil {
		return nil, gqlerror.Errorf("operation is not defined")
	}
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
