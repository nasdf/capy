package graphql

import (
	"fmt"

	"github.com/nasdf/capy/plan"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// ParseQuery parses a GraphQL query into a plan.Request.
func ParseQuery(schema *ast.Schema, params QueryParams) (plan.Node, error) {
	doc, errs := gqlparser.LoadQuery(schema, params.Query)
	if errs != nil {
		return nil, errs
	}

	var op *ast.OperationDefinition
	if params.OperationName != "" {
		doc.Operations.ForName(params.OperationName)
	} else if len(doc.Operations) == 1 {
		op = doc.Operations[0]
	}
	if op == nil {
		return nil, fmt.Errorf("operation is not defined")
	}

	fields := graphql.CollectFields(&graphql.OperationContext{
		Doc:       doc,
		RawQuery:  params.Query,
		Variables: params.Variables,
	}, op.SelectionSet, nil)

	req, err := parseRequest(fields, params.Variables)
	if err != nil {
		return nil, err
	}

	switch op.Operation {
	case ast.Query:
		return plan.Query(req), nil
	case ast.Mutation:
		return plan.Mutation(req), nil
	default:
		return nil, fmt.Errorf("unsupported operation %s", op.Operation)
	}
}

func parseRequest(fields []graphql.CollectedField, variables map[string]any) (plan.Request, error) {
	req := plan.Request{
		Fields: make(map[string]plan.Request),
	}
	for _, f := range fields {
		field, err := parseRequestField(f.Field, variables)
		if err != nil {
			return plan.Request{}, err
		}
		req.Fields[f.Alias] = field
	}
	return req, nil
}

func parseRequestField(field *ast.Field, variables map[string]any) (plan.Request, error) {
	fields := make(map[string]plan.Request)
	for _, s := range field.SelectionSet {
		field := s.(*ast.Field)
		child, err := parseRequestField(field, variables)
		if err != nil {
			return plan.Request{}, err
		}
		fields[field.Alias] = child
	}
	return plan.Request{
		Name:      field.Name,
		Arguments: field.ArgumentMap(variables),
		Fields:    fields,
	}, nil
}
