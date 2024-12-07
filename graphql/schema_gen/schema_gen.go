package schema_gen

import (
	"bytes"
	_ "embed"
	"text/template"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

//go:embed schema.graphql
var schemaTemplateSource string

//go:embed prelude.graphql
var preludeSource string

// Execute creates a GraphQL schema from the given IPLD schema.TypeSystem.
func Execute(input string) (*ast.Schema, error) {
	inputSource := ast.Source{Input: input}
	inputSchema, err := gqlparser.LoadSchema(&inputSource)
	if err != nil {
		return nil, err
	}
	schemaTemplate, err := template.New("").Parse(schemaTemplateSource)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := schemaTemplate.Execute(&out, inputSchema); err != nil {
		return nil, err
	}
	preludeSource := ast.Source{Input: preludeSource}
	outputSource := ast.Source{Input: out.String()}
	return gqlparser.LoadSchema(&preludeSource, &inputSource, &outputSource)
}
