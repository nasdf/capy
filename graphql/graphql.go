package graphql

import (
	"context"

	"github.com/nasdf/capy/core"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// QueryResponse contains all of the fields for a response.
type QueryResponse struct {
	Data       any           `json:"data,omitempty"`
	Errors     gqlerror.List `json:"errors,omitempty"`
	Extensions any           `json:"extensions,omitempty"`
}

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// Execute runs the query and returns a node containing the result of the query operation.
func Execute(ctx context.Context, repo *core.Repository, params QueryParams) QueryResponse {
	exe, errs := NewRequest(ctx, repo, params)
	if errs != nil {
		return NewQueryResponse(nil, errs)
	}
	data, err := exe.Execute(ctx)
	if err != nil {
		return NewQueryResponse(nil, err)
	}
	if exe.operation.Operation != ast.Mutation {
		return NewQueryResponse(data, nil)
	}
	// commit the transaction
	hash, err := exe.tx.Commit(ctx)
	if err != nil {
		return NewQueryResponse(nil, err)
	}
	err = repo.Merge(ctx, hash)
	if err != nil {
		return NewQueryResponse(nil, err)
	}
	return NewQueryResponse(data, nil)
}

// NewQueryResponse returns a new GraphQL compliant response.
func NewQueryResponse(data any, err error) QueryResponse {
	response := QueryResponse{
		Data: data,
	}
	switch t := err.(type) {
	case nil:
		response.Errors = nil
	case gqlerror.List:
		response.Errors = t
	case *gqlerror.Error:
		response.Errors = gqlerror.List{t}
	default:
		response.Errors = gqlerror.List{gqlerror.Wrap(err)}
	}
	return response
}
