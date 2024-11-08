package types

import (
	"errors"
	"fmt"

	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// RootTypeName is the name of the root struct type.
	RootTypeName        = "__Root"
	RootSchemaFieldName = "Schema"
)

// baseTypes contains all of the scalar and list types.
var baseTypes = []ipldschema.Type{
	ipldschema.SpawnInt("Int"),
	ipldschema.SpawnFloat("Float"),
	ipldschema.SpawnBool("Boolean"),
	ipldschema.SpawnString("String"),
	ipldschema.SpawnList("[Int]", "Int", true),
	ipldschema.SpawnList("[Int!]", "Int", false),
	ipldschema.SpawnList("[Float]", "Float", true),
	ipldschema.SpawnList("[Float!]", "Float", false),
	ipldschema.SpawnList("[Boolean]", "Boolean", true),
	ipldschema.SpawnList("[Boolean!]", "Boolean", false),
	ipldschema.SpawnList("[String]", "String", true),
	ipldschema.SpawnList("[String!]", "String", false),
}

func accumulate(schema *ast.Schema, collections []string) (*ipldschema.TypeSystem, error) {
	system := ipldschema.MustTypeSystem(baseTypes...)
	for _, d := range schema.Types {
		if !d.BuiltIn {
			accumulateSchemaType(system, schema, d)
		}
	}
	rootFields := []ipldschema.StructField{
		ipldschema.SpawnStructField(RootSchemaFieldName, "String", false, false),
	}
	for _, n := range collections {
		rootFields = append(rootFields, ipldschema.SpawnStructField(n, fmt.Sprintf("[&%s]", n), false, false))
	}
	system.Accumulate(ipldschema.SpawnStruct(RootTypeName, rootFields, ipldschema.SpawnStructRepresentationMap(nil)))
	errs := system.ValidateGraph()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return system, nil
}

func accumulateSchemaType(ts *ipldschema.TypeSystem, s *ast.Schema, d *ast.Definition) {
	switch d.Kind {
	case ast.Object:
		accumulateSchemaStructType(ts, s, d)
	case ast.Enum:
		accumulateSchemaEnumType(ts, d)
	default:
		panic(fmt.Sprintf("unsupported kind %s", d.Kind))
	}
}

func accumulateSchemaEnumType(ts *ipldschema.TypeSystem, d *ast.Definition) {
	members := make([]string, len(d.EnumValues))
	repr := make(ipldschema.EnumRepresentation_String)
	for i, v := range d.EnumValues {
		members[i] = v.Name
		repr[v.Name] = v.Name
	}
	ts.Accumulate(ipldschema.SpawnEnum(d.Name, members, repr))
}

func accumulateSchemaStructType(ts *ipldschema.TypeSystem, s *ast.Schema, d *ast.Definition) {
	fields := make([]ipldschema.StructField, len(d.Fields))
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
		fields[i] = ipldschema.SpawnStructField(field.Name, name, !nonNull, !nonNull)
	}
	ts.Accumulate(ipldschema.SpawnStruct(d.Name, fields, ipldschema.SpawnStructRepresentationMap(nil)))
	ts.Accumulate(ipldschema.SpawnList(fmt.Sprintf("[%s]", d.Name), d.Name, true))
	ts.Accumulate(ipldschema.SpawnList(fmt.Sprintf("[%s!]", d.Name), d.Name, false))

	ts.Accumulate(ipldschema.SpawnLinkReference("&"+d.Name, d.Name))
	ts.Accumulate(ipldschema.SpawnList(fmt.Sprintf("[&%s]", d.Name), "&"+d.Name, true))
	ts.Accumulate(ipldschema.SpawnList(fmt.Sprintf("[&%s!]", d.Name), "&"+d.Name, false))
}
