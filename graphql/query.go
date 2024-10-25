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
		Fields: make([]plan.RequestField, len(fields)),
	}
	for i, f := range fields {
		field, err := parseRequestField(f.Field, variables)
		if err != nil {
			return plan.Request{}, err
		}
		req.Fields[i] = field
	}
	return req, nil
}

func parseRequestField(field *ast.Field, variables map[string]any) (plan.RequestField, error) {
	children := make([]plan.RequestField, len(field.SelectionSet))
	for i, s := range field.SelectionSet {
		child, err := parseRequestField(s.(*ast.Field), variables)
		if err != nil {
			return plan.RequestField{}, err
		}
		children[i] = child
	}
	return plan.RequestField{
		Name:      field.Name,
		Alias:     field.Alias,
		Arguments: field.ArgumentMap(variables),
		Children:  children,
	}, nil
}
