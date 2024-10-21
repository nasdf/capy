package schema

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	//go:embed schema.graphql
	schemaSource        string
	schemaTemplate      = template.Must(template.New("schema.graphql").Funcs(schemaTemplateFuncs).Parse(schemaSource))
	schemaTemplateFuncs = template.FuncMap(map[string]any{
		"nameForType":        nameForType,
		"nameForCreateInput": nameForCreateInput,
	})
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

func nameForCreateInput(t schema.Type) string {
	switch v := t.(type) {
	case *schema.TypeLink:
		if !v.HasReferencedType() {
			return v.Name()
		}
		return v.ReferencedType().Name() + "CreateInput"

	case *schema.TypeList:
		if v.ValueIsNullable() {
			return fmt.Sprintf("[%s!]", nameForCreateInput(v.ValueType()))
		}
		return fmt.Sprintf("[%s]", nameForCreateInput(v.ValueType()))

	default:
		return t.Name()
	}
}

func nameForType(t schema.Type) string {
	switch v := t.(type) {
	case *schema.TypeLink:
		if !v.HasReferencedType() {
			return v.Name()
		}
		return v.ReferencedType().Name()

	case *schema.TypeList:
		if v.ValueIsNullable() {
			return fmt.Sprintf("[%s!]", nameForType(v.ValueType()))
		}
		return fmt.Sprintf("[%s]", nameForType(v.ValueType()))

	default:
		return t.Name()
	}
}
