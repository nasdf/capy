package graphql

import (
	"bytes"
	_ "embed"
	"text/template"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

//go:embed schema.graphql
var schemaTemplateSource string

// GenerateSchema creates a GraphQL schema from the given IPLD schema.TypeSystem.
func GenerateSchema(input string) (*ast.Schema, error) {
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
	outputSource := ast.Source{Input: out.String()}
	return gqlparser.LoadSchema(&inputSource, &outputSource)
}
