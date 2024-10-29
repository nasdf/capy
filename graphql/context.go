package graphql

import (
	"fmt"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

type execContext struct {
	doc       *ast.QueryDocument
	operation *ast.OperationDefinition
	schema    *ast.Schema
	query     string
	variables map[string]any
}

func buildExecContext(schema *ast.Schema, params QueryParams) (*execContext, error) {
	doc, errs := gqlparser.LoadQuery(schema, params.Query)
	if errs != nil {
		return nil, errs
	}

	var operation *ast.OperationDefinition
	if params.OperationName != "" {
		operation = doc.Operations.ForName(params.OperationName)
	} else if len(doc.Operations) == 1 {
		operation = doc.Operations[0]
	}
	if operation == nil {
		return nil, fmt.Errorf("operation is not defined")
	}

	return &execContext{
		operation: operation,
		doc:       doc,
		schema:    schema,
		query:     params.Query,
		variables: params.Variables,
	}, nil
}

func (c *execContext) collectFields(sel ast.SelectionSet, satisfies []string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  c.query,
		Variables: c.variables,
		Doc:       c.doc,
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
