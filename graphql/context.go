package graphql

import (
	"context"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/link"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type contextKey string

var (
	idContextKey   = contextKey("id")
	linkContextKey = contextKey("link")
)

const (
	createOperationPrefix = "create"
	updateOperationPrefix = "update"
	deleteOperationPrefix = "delete"
	listOperationPrefix   = "list"
	findOperationPrefix   = "find"
)

type Context struct {
	branch    *core.Branch
	schema    *ast.Schema
	query     *ast.QueryDocument
	operation *ast.OperationDefinition
	params    QueryParams
}

func NewContext(ctx context.Context, store *core.Store, params QueryParams) (*Context, gqlerror.List) {
	query, errs := gqlparser.LoadQuery(store.Schema(), params.Query)
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
	var branch *core.Branch
	switch rev := operation.Directives.ForName("revision"); rev {
	case nil:
		b, err := store.Branch(ctx, store.Head())
		if err != nil {
			return nil, gqlerror.List{gqlerror.Wrap(err)}
		}
		branch = b
	default:
		l, err := link.Parse(rev.Arguments.ForName("link").Value.Raw)
		if err != nil {
			return nil, gqlerror.List{gqlerror.Wrap(err)}
		}
		b, err := store.Branch(ctx, l)
		if err != nil {
			return nil, gqlerror.List{gqlerror.Wrap(err)}
		}
		branch = b
	}
	return &Context{
		schema:    store.Schema(),
		branch:    branch,
		query:     query,
		operation: operation,
		params:    params,
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
		err = e.branch.Merge(ctx)
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
