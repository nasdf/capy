package graphql

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"github.com/nasdf/capy/types"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

//go:embed schema.graphql
var schemaTemplateSource string

// GenerateSchema creates a GraphQL schema from the given IPLD schema.TypeSystem.
func GenerateSchema(system *types.System) (*ast.Schema, error) {
	templateFuncs := template.FuncMap(map[string]any{
		"nameForType":        nameForType,
		"nameForCreateInput": nameForCreateInput,
	})
	schemaTemplate, err := template.New("").Funcs(templateFuncs).Parse(schemaTemplateSource)
	if err != nil {
		return nil, err
	}
	schemaTypes := make(map[string]schema.Type)
	for _, n := range system.Collections() {
		schemaTypes[n] = system.Type(n)
	}
	var out bytes.Buffer
	if err := schemaTemplate.Execute(&out, schemaTypes); err != nil {
		return nil, err
	}
	return gqlparser.LoadSchema(&ast.Source{Input: out.String()})
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
