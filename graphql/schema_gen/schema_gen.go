package schema_gen

import (
	_ "embed"
	"fmt"
	"io"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

//go:embed prelude.graphql
var preludeSource string

// Execute creates a GraphQL schema from the given IPLD schema.TypeSystem.
func Execute(input string) (*ast.Schema, error) {
	inputSource := ast.Source{Input: input}
	inputSchema, err := gqlparser.LoadSchema(&inputSource)
	if err != nil {
		return nil, err
	}
	var output strings.Builder
	for _, def := range inputSchema.Types {
		if def.BuiltIn || def.Kind != ast.Object {
			continue
		}
		_, err = documentType(def, &output)
		if err != nil {
			return nil, err
		}
		_, err = documentFilterInput(def, &output)
		if err != nil {
			return nil, err
		}
		_, err = documentListFilterInput(def, &output)
		if err != nil {
			return nil, err
		}
		_, err = documentPatchInput(def, &output)
		if err != nil {
			return nil, err
		}
		_, err = documentListPatchInput(def, &output)
		if err != nil {
			return nil, err
		}
		_, err = documentCreateInput(def, inputSchema, &output)
		if err != nil {
			return nil, err
		}
	}
	_, err = queryType(inputSchema, &output)
	if err != nil {
		return nil, err
	}
	_, err = mutationType(inputSchema, &output)
	if err != nil {
		return nil, err
	}
	preludeSource := ast.Source{Input: preludeSource, BuiltIn: true}
	outputSource := ast.Source{Input: output.String()}
	return gqlparser.LoadSchema(&preludeSource, &inputSource, &outputSource)
}

// queryType defines the query operations
func queryType(schema *ast.Schema, w io.Writer) (int, error) {
	fields := make([]string, 0)
	for _, def := range schema.Types {
		if def.BuiltIn || def.Kind != ast.Object {
			continue
		}
		fields = append(fields, fmt.Sprintf(`
	"""
    List %[1]s documents.
    """
    list%[1]s(filter: %[1]sFilterInput): [%[1]s]
    """
    Find a %[1]s document.
    """
    find%[1]s(id: ID!): %[1]s`, def.Name))
	}
	return fmt.Fprintf(w, `extend type Query {
	%s
}`, strings.Join(fields, "\n"))
}

// mutationType defines the mutation operations
func mutationType(schema *ast.Schema, w io.Writer) (int, error) {
	fields := make([]string, 0)
	for _, def := range schema.Types {
		if def.BuiltIn || def.Kind != ast.Object {
			continue
		}
		fields = append(fields, fmt.Sprintf(`
    """
    Create a %[1]s document.
    """
    create%[1]s(data: %[1]sCreateInput): %[1]s
    """
    Delete %[1]s documents.
    """
    delete%[1]s(filter: %[1]sFilterInput): [%[1]s]
    """
    Update %[1]s documents.
    """
    update%[1]s(filter: %[1]sFilterInput, patch: %[1]sPatchInput): [%[1]s]
		`, def.Name))
	}
	return fmt.Fprintf(w, `extend type Mutation {
	%s
}`, strings.Join(fields, "\n"))
}

// documentType extends a document type with generated fields
func documentType(def *ast.Definition, w io.Writer) (int, error) {
	return fmt.Fprintf(w, `extend type %s {
	"""
	The unique identifier of this document.
	"""
	id: ID!
	"""
	The hash of this document.
	"""
	hash: String!
}`, def.Name)
}

// documentFilterInput is the input type for filtering documents of this type.
func documentFilterInput(def *ast.Definition, w io.Writer) (int, error) {
	fields := make([]string, len(def.Fields))
	for i, field := range def.Fields {
		if field.Type.Elem != nil {
			fields[i] = fmt.Sprintf("%s: %sListFilterInput", field.Name, field.Type.Elem.Name())
		} else {
			fields[i] = fmt.Sprintf("%s: %sFilterInput", field.Name, field.Type.Name())
		}
	}
	return fmt.Fprintf(w, `
"""
Input for filtering %[1]s documents.
"""
input %[1]sFilterInput {
    """
    Matches if all filters match.
    """
    and: [%[1]sFilterInput!]
    """
    Matches if one filter matches.
    """
    or: [%[1]sFilterInput!]
    """
    Matches if the filter does not match.
    """
    not: %[1]sFilterInput
	%s
}`, def.Name, strings.Join(fields, "\n"))
}

// documentListFilterInput is the input type for filtering lists of documents of this type.
func documentListFilterInput(def *ast.Definition, w io.Writer) (int, error) {
	return fmt.Fprintf(w, `
"""
Input for filtering %[1]s document lists.
"""
input %[1]sListFilterInput {
    """
    Matches if all field values match.
    """
    all: [%[1]sFilterInput!]
    """
    Matches if any field values match.
    """
    any: [%[1]sFilterInput!]
    """
    Matches if no field values match.
    """
    none: [%[1]sFilterInput!]
}`, def.Name)
}

// documentPatchInput is the input type for patching documents of this type
func documentPatchInput(def *ast.Definition, w io.Writer) (int, error) {
	fields := make([]string, len(def.Fields))
	for i, field := range def.Fields {
		if field.Type.Elem != nil {
			fields[i] = fmt.Sprintf("%s: %sListPatchInput", field.Name, field.Type.Elem.Name())
		} else {
			fields[i] = fmt.Sprintf("%s: %sPatchInput", field.Name, field.Type.Name())
		}
	}
	return fmt.Fprintf(w, `
"""
Input for patching %[1]s documents.
"""
input %[1]sPatchInput {
    """
    This field can be used to create relationships with existing %[1]s documents.
    """
    id: IDPatchInput
	%s
}`, def.Name, strings.Join(fields, "\n"))
}

// documentListPatchInput is the input type for patching document lists of this type.
func documentListPatchInput(def *ast.Definition, w io.Writer) (int, error) {
	return fmt.Fprintf(w, `
"""
Input for patching %[1]s fields and documents.
"""
input %[1]sListPatchInput {
    """
    Sets the value of the field.
    """
    set: [%[1]sCreateInput]
    """
    Append values to the field.
    """
    append: [%[1]sCreateInput]
    """
    Filter values in the field.
    """
    filter: %[1]sListFilterInput
}`, def.Name)
}

// documentCreateInput is input for creating documents of this type.
func documentCreateInput(def *ast.Definition, schema *ast.Schema, w io.Writer) (int, error) {
	fields := make([]string, len(def.Fields))
	for i, field := range def.Fields {
		def := schema.Types[field.Type.Name()]
		if def.IsLeafType() {
			fields[i] = fmt.Sprintf("%s: %s", field.Name, field.Type.String())
		} else if field.Type.Elem != nil {
			fields[i] = fmt.Sprintf("%s: [%sCreateInput!]", field.Name, field.Type.Name())
		} else {
			fields[i] = fmt.Sprintf("%s: %sCreateInput", field.Name, field.Type.Name())
		}
	}
	return fmt.Fprintf(w, `
"""
Input for creating %[1]s documents.
"""
input %[1]sCreateInput {
    """
    This field can be used to create relationships with existing %[1]s documents.
    """
    id: ID
	%s
}`, def.Name, strings.Join(fields, "\n"))
}
