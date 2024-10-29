package graphql

import (
	"fmt"

	"github.com/nasdf/capy/plan"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
)

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// QueryResponse contains the fields expected from a GraphQL http response.
type QueryResponse struct {
	Data   any      `json:"data"`
	Errors []string `json:"errors,omitempty"`
}

// ParseQuery parses a GraphQL query into a plan.Request.
func ParseQuery(schema *ast.Schema, params QueryParams) (plan.Node, error) {
	exec, err := buildExecContext(schema, params)
	if err != nil {
		return nil, err
	}
	fields := exec.collectFields(exec.operation.SelectionSet, nil)
	if IsIntrospect(fields) {
		return plan.Introspect(Introspect(exec)), nil
	}
	req, err := parseRequest(exec, fields)
	if err != nil {
		return nil, err
	}
	switch exec.operation.Operation {
	case ast.Query:
		return plan.Query(req), nil
	case ast.Mutation:
		return plan.Mutation(req), nil
	default:
		return nil, fmt.Errorf("unsupported operation %s", exec.operation.Operation)
	}
}

func parseRequest(exec *execContext, fields []graphql.CollectedField) (plan.Request, error) {
	req := plan.Request{
		Fields: make(map[string]plan.Request),
	}
	for _, f := range fields {
		field, err := parseRequestField(exec, f.Field)
		if err != nil {
			return plan.Request{}, err
		}
		req.Fields[f.Alias] = field
	}
	return req, nil
}

func parseRequestField(exec *execContext, field *ast.Field) (plan.Request, error) {
	fields := make(map[string]plan.Request)
	for _, s := range field.SelectionSet {
		field := s.(*ast.Field)
		child, err := parseRequestField(exec, field)
		if err != nil {
			return plan.Request{}, err
		}
		fields[field.Alias] = child
	}
	return plan.Request{
		Name:      field.Name,
		Arguments: field.ArgumentMap(exec.variables),
		Fields:    fields,
	}, nil
}
