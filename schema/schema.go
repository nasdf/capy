package schema

import (
	"bytes"
	_ "embed"
	"strings"
	"text/template"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	//go:embed schema.graphql
	schemaSource   string
	schemaTemplate = template.Must(template.New("schema.graphql").Parse(schemaSource))
)

// Generate creates a GraphQL schema from the given TypeSystem.
func Generate(ts *schema.TypeSystem) (*ast.Schema, error) {
	schemaTypes := make(map[string]schema.Type)
	for n, v := range ts.GetTypes() {
		if !strings.HasPrefix(n, "__") {
			schemaTypes[n] = v
		}
	}
	var out bytes.Buffer
	if err := schemaTemplate.Execute(&out, schemaTypes); err != nil {
		return nil, err
	}
	return gqlparser.LoadSchema(&ast.Source{
		Name:  "schema.graphql",
		Input: out.String(),
	})
}
