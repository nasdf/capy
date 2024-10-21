package schema

import (
	"errors"
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

// RootTypeName is the name of the root struct type.
const RootTypeName = "__Root"

// baseTypes contains all of the scalar and list types.
var baseTypes = []schema.Type{
	schema.SpawnInt("Int"),
	schema.SpawnFloat("Float"),
	schema.SpawnBool("Boolean"),
	schema.SpawnString("String"),
	schema.SpawnList("[Int]", "Int", true),
	schema.SpawnList("[Int!]", "Int", false),
	schema.SpawnList("[Float]", "Float", true),
	schema.SpawnList("[Float!]", "Float", false),
	schema.SpawnList("[Boolean]", "Boolean", true),
	schema.SpawnList("[Boolean!]", "Boolean", false),
	schema.SpawnList("[String]", "String", true),
	schema.SpawnList("[String!]", "String", false),
}

// SpawnTypeSystem returns a new TypeSystem containing the user defined types.
func SpawnTypeSystem(s *ast.Schema) (*schema.TypeSystem, error) {
	typeSys := schema.MustTypeSystem(baseTypes...)
	for _, d := range s.Types {
		if !d.BuiltIn {
			accumulateSchemaType(typeSys, s, d)
		}
	}

	var rootFields []schema.StructField
	for n, d := range s.Types {
		if !d.BuiltIn && d.Kind == ast.Object {
			rootFields = append(rootFields, schema.SpawnStructField(n, fmt.Sprintf("[&%s]", n), false, false))
		}
	}
	typeSys.Accumulate(schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))

	errs := typeSys.ValidateGraph()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return typeSys, nil
}

func accumulateSchemaType(ts *schema.TypeSystem, s *ast.Schema, d *ast.Definition) {
	switch d.Kind {
	case ast.Object:
		accumulateSchemaStructType(ts, s, d)
	case ast.Enum:
		accumulateSchemaEnumType(ts, d)
	default:
		panic(fmt.Sprintf("unsupported kind %s", d.Kind))
	}
}

func accumulateSchemaEnumType(ts *schema.TypeSystem, d *ast.Definition) {
	members := make([]string, len(d.EnumValues))
	repr := make(schema.EnumRepresentation_String)
	for i, v := range d.EnumValues {
		members[i] = v.Name
		repr[v.Name] = v.Name
	}
	ts.Accumulate(schema.SpawnEnum(d.Name, members, repr))
}

func accumulateSchemaStructType(ts *schema.TypeSystem, s *ast.Schema, d *ast.Definition) {
	fields := make([]schema.StructField, len(d.Fields))
	for i, field := range d.Fields {
		var name string
		var nonNull bool
		if field.Type.Elem != nil {
			name = field.Type.Elem.NamedType
			nonNull = field.Type.Elem.NonNull
		} else {
			name = field.Type.NamedType
			nonNull = field.Type.NonNull
		}
		t, ok := s.Types[name]
		if ok && t.Kind == ast.Object {
			name = "&" + name
		}
		if nonNull {
			name = name + "!"
		}
		if field.Type.Elem != nil {
			name = fmt.Sprintf("[%s]", name)
		}
		fields[i] = schema.SpawnStructField(field.Name, name, !nonNull, !nonNull)
	}
	ts.Accumulate(schema.SpawnStruct(d.Name, fields, schema.SpawnStructRepresentationMap(nil)))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s]", d.Name), d.Name, true))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s!]", d.Name), d.Name, false))

	ts.Accumulate(schema.SpawnLinkReference("&"+d.Name, d.Name))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[&%s]", d.Name), "&"+d.Name, true))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[&%s!]", d.Name), "&"+d.Name, false))
}
