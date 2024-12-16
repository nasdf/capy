package graphql

import (
	"context"
	"encoding/hex"

	"github.com/nasdf/capy/core"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type contextKey string

var (
	idContextKey   = contextKey("id")
	hashContextKey = contextKey("hash")
)

const (
	createOperationPrefix = "create"
	updateOperationPrefix = "update"
	deleteOperationPrefix = "delete"
	listOperationPrefix   = "list"
	findOperationPrefix   = "find"
)

type Request struct {
	tx        *core.Transaction
	schema    *ast.Schema
	query     *ast.QueryDocument
	operation *ast.OperationDefinition
	params    QueryParams
}

func NewRequest(ctx context.Context, repo *core.Repository, params QueryParams) (*Request, error) {
	query, errs := gqlparser.LoadQuery(repo.Schema(), params.Query)
	if errs != nil {
		return nil, errs
	}
	var operation *ast.OperationDefinition
	if params.OperationName != "" {
		operation = query.Operations.ForName(params.OperationName)
	} else if len(query.Operations) == 1 {
		operation = query.Operations[0]
	}
	if operation == nil {
		return nil, gqlerror.Errorf("operation is not defined")
	}
	hash := repo.Head()
	if rev := operation.Directives.ForName("revision"); rev != nil {
		b, err := hex.DecodeString(rev.Arguments.ForName("hash").Value.Raw)
		if err != nil {
			return nil, err
		}
		hash = b
	}
	tx, err := repo.Transaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	return &Request{
		tx:        tx,
		schema:    repo.Schema(),
		query:     query,
		operation: operation,
		params:    params,
	}, nil
}

func (e *Request) Execute(ctx context.Context) (any, error) {
	switch e.operation.Operation {
	case ast.Mutation:
		return e.executeMutation(ctx, e.operation.SelectionSet)
	case ast.Query:
		return e.executeQuery(ctx, e.operation.SelectionSet)
	default:
		return nil, gqlerror.Errorf("unsupported operation %s", e.operation.Operation)
	}
}

func (e *Request) collectFields(sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  e.params.Query,
		Variables: e.params.Variables,
		Doc:       e.query,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
