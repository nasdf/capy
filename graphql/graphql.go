package graphql

import (
	"context"

	"github.com/rodent-software/capy/core"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

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

// QueryResponse contains all of the fields for a response.
type QueryResponse struct {
	Data       any            `json:"data,omitempty"`
	Errors     gqlerror.List  `json:"errors,omitempty"`
	Extensions map[string]any `json:"extensions,omitempty"`
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

// ToMap converts the query response to a standard go map.
//
// This is used to convert values in environments such as WASM.
func (r QueryResponse) ToMap() map[string]any {
	out := make(map[string]any)
	if r.Data != nil {
		out["data"] = r.Data
	}
	if len(r.Extensions) > 0 {
		out["extensions"] = r.Extensions
	}
	errors := make([]map[string]any, len(r.Errors))
	for i, e := range r.Errors {
		err := make(map[string]any)
		err["message"] = e.Message
		if e.Path != nil {
			err["path"] = e.Path.String()
		}
		if len(e.Extensions) > 0 {
			err["extensions"] = e.Extensions
		}
		locations := make([]map[string]any, len(e.Locations))
		for i, l := range e.Locations {
			locations[i] = map[string]any{
				"line":   l.Line,
				"column": l.Column,
			}
		}
		if len(locations) > 0 {
			err["locations"] = locations
		}
		errors[i] = err
	}
	if len(errors) > 0 {
		out["errors"] = errors
	}
	return out
}
