package query

import (
	"fmt"
	"strings"

	"github.com/nasdf/capy/plan"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	createOpPrefix       = "create"
	createOpInputArgName = "input"
)

type Params struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// Parse creates a plan.Node for the given query.
func Parse(schema *ast.Schema, params *Params) (plan.Node, error) {
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
		Variables: params.Variables,
	}, op.SelectionSet, nil)

	switch op.Operation {
	case ast.Query:
		return parseQuery(fields)
	case ast.Mutation:
		return parseMutation(fields, params.Variables)
	default:
		return nil, fmt.Errorf("operation not supported: %s", op.Operation)
	}
}

func parseQuery(fields []graphql.CollectedField) (plan.Node, error) {
	sel, err := querySelector(fields).Selector()
	if err != nil {
		return nil, err
	}
	return plan.Select(sel), nil
}

func parseMutation(fields []graphql.CollectedField, variables map[string]any) (plan.Node, error) {
	ops := make([]plan.Node, len(fields))
	for i, f := range fields {
		if strings.HasPrefix(f.Name, createOpPrefix) {
			ops[i] = parseCreateOperation(f, variables)
			// remove the operation from the field name so we build the correct selector
			// TODO: this can be handled better by keeping track of aliases and remapping
			f.Name = strings.TrimPrefix(f.Name, createOpPrefix)
		}
	}
	sel, err := querySelector(fields).Selector()
	if err != nil {
		return nil, err
	}
	return plan.Select(sel, ops...), nil
}

func parseCreateOperation(field graphql.CollectedField, variables map[string]any) plan.Node {
	args := field.ArgumentMap(variables)
	collection := strings.TrimPrefix(field.Name, createOpPrefix)
	input := args[createOpInputArgName]
	return plan.Create(collection, input)
}
