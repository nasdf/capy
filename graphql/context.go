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

type Context struct {
	collections *core.Collections
	schema      *ast.Schema
	query       *ast.QueryDocument
	operation   *ast.OperationDefinition
	params      QueryParams
}

func NewContext(collections *core.Collections, schema *ast.Schema, params QueryParams) (*Context, gqlerror.List) {
	query, errs := gqlparser.LoadQuery(schema, params.Query)
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
		return nil, gqlerror.List{gqlerror.Errorf("operation is not defined")}
	}
	return &Context{
		collections: collections,
		schema:      schema,
		query:       query,
		params:      params,
		operation:   operation,
	}, nil
}

func (e *Context) Execute(ctx context.Context) (datamodel.Node, error) {
	res := basicnode.Prototype.Map.NewBuilder()
	switch e.operation.Operation {
	case ast.Mutation:
		err := e.executeMutation(ctx, e.operation.SelectionSet, res)
		if err != nil {
			return nil, err
		}
	case ast.Query:
		err := e.executeQuery(ctx, e.operation.SelectionSet, res)
		if err != nil {
			return nil, err
		}
	default:
		return nil, gqlerror.Errorf("unsupported operation %s", e.operation.Operation)
	}
	return res.Build(), nil
}

func (e *Context) collectFields(sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  e.params.Query,
		Variables: e.params.Variables,
		Doc:       e.query,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
