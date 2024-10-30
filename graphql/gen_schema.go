package graphql

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed gen_schema.graphql
var genSchemaSource string

// GenerateSchema creates a GraphQL schema from the given IPLD schema.TypeSystem.
func GenerateSchema(ts *schema.TypeSystem) (string, error) {
	templateFuncs := template.FuncMap(map[string]any{
		"nameForType":        nameForType,
		"nameForCreateInput": nameForCreateInput,
	})
	schemaTemplate, err := template.New("").Funcs(templateFuncs).Parse(genSchemaSource)
	if err != nil {
		return "", err
	}
	schemaTypes := make(map[string]schema.Type)
	for n, v := range ts.GetTypes() {
		if !strings.HasPrefix(n, "__") {
			schemaTypes[n] = v
		}
	}
	var out bytes.Buffer
	if err := schemaTemplate.Execute(&out, schemaTypes); err != nil {
		return "", err
	}
	return out.String(), nil
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
