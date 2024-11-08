package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/introspection"
	"github.com/vektah/gqlparser/v2/ast"
)

func (e *executionContext) introspectSchema(obj *introspection.Schema, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(sel, "__Schema")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Schema"
		case "types":
			result[field.Alias] = e.introspectSchemaTypes(field.SelectionSet)
		case "queryType":
			result[field.Alias] = e.introspectType(obj.QueryType(), field.SelectionSet)
		case "mutationType":
			result[field.Alias] = e.introspectType(obj.MutationType(), field.SelectionSet)
		case "subscriptionType":
			result[field.Alias] = e.introspectType(obj.SubscriptionType(), field.SelectionSet)
		case "directives":
			result[field.Alias] = e.introspectDirectives(obj.Directives(), field.SelectionSet)
		}
	}
	return result
}

func (e *executionContext) introspectType(obj *introspection.Type, sel ast.SelectionSet) map[string]any {
	if obj == nil {
		return nil
	}
	fields := e.collectFields(sel, "__Type")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Type"
		case "kind":
			result[field.Alias] = obj.Kind()
		case "name":
			result[field.Alias] = obj.Name()
		case "description":
			result[field.Alias] = obj.Description()
		case "fields":
			result[field.Alias] = e.introspectTypeFields(obj, field.Field)
		case "interfaces":
			result[field.Alias] = e.introspectTypes(obj.Interfaces(), field.SelectionSet)
		case "possibleTypes":
			result[field.Alias] = e.introspectTypes(obj.PossibleTypes(), field.SelectionSet)
		case "enumValues":
			result[field.Alias] = e.introspectTypeEnumValues(obj, field.Field)
		case "inputFields":
			result[field.Alias] = e.introspectInputValues(obj.InputFields(), field.SelectionSet)
		case "ofType":
			result[field.Alias] = e.introspectType(obj.OfType(), field.SelectionSet)
		}
	}
	return result
}

func (e *executionContext) introspectField(obj *introspection.Field, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(sel, "__Field")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Field"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "args":
			result[field.Alias] = e.introspectInputValues(obj.Args, field.SelectionSet)
		case "type":
			result[field.Alias] = e.introspectType(obj.Type, field.SelectionSet)
		case "isDeprecated":
			result[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			result[field.Alias] = obj.DeprecationReason()
		}
	}
	return result
}

func (e *executionContext) introspectInputValue(obj introspection.InputValue, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(sel, "__InputValue")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__InputValue"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "type":
			result[field.Alias] = e.introspectType(obj.Type, field.SelectionSet)
		case "defaultValue":
			result[field.Alias] = obj.DefaultValue
		}
	}
	return result
}

func (e *executionContext) introspectEnumValue(obj introspection.EnumValue, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(sel, "__EnumValue")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__EnumValue"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "isDeprecated":
			result[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			result[field.Alias] = obj.DeprecationReason()
		}
	}
	return result
}

func (e *executionContext) introspectDirective(obj introspection.Directive, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(sel, "__Directive")
	result := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Directive"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "locations":
			result[field.Alias] = obj.Locations
		case "args":
			result[field.Alias] = e.introspectInputValues(obj.Args, field.SelectionSet)
		}
	}
	return result
}

func (e *executionContext) introspectDirectives(obj []introspection.Directive, sel ast.SelectionSet) []any {
	result := make([]any, len(obj))
	for i, d := range obj {
		result[i] = e.introspectDirective(d, sel)
	}
	return result
}

func (e *executionContext) introspectSchemaTypes(sel ast.SelectionSet) []any {
	result := make([]any, 0, len(e.schema.Types))
	for _, t := range e.schema.Types {
		result = append(result, e.introspectType(introspection.WrapTypeFromDef(e.schema, t), sel))
	}
	return result
}

func (e *executionContext) introspectTypeEnumValues(obj *introspection.Type, field *ast.Field) []any {
	args := field.ArgumentMap(e.params.Variables)
	res := obj.EnumValues(args["includeDeprecated"].(bool))
	return e.introspectEnumValues(res, field.SelectionSet)
}

func (e *executionContext) introspectEnumValues(obj []introspection.EnumValue, sel ast.SelectionSet) []any {
	result := make([]any, len(obj))
	for i, v := range obj {
		result[i] = e.introspectEnumValue(v, sel)
	}
	return result
}

func (e *executionContext) introspectInputValues(obj []introspection.InputValue, sel ast.SelectionSet) []any {
	result := make([]any, len(obj))
	for i, o := range obj {
		result[i] = e.introspectInputValue(o, sel)
	}
	return result
}

func (e *executionContext) introspectQuerySchema(field graphql.CollectedField) map[string]any {
	typ := introspection.WrapSchema(e.schema)
	return e.introspectSchema(typ, field.SelectionSet)
}

func (e *executionContext) introspectQueryType(field graphql.CollectedField) map[string]any {
	args := field.ArgumentMap(e.params.Variables)
	name := args["name"].(string)
	typ := introspection.WrapTypeFromDef(e.schema, e.schema.Types[name])
	return e.introspectType(typ, field.SelectionSet)
}

func (e *executionContext) introspectTypeFields(typ *introspection.Type, field *ast.Field) []any {
	args := field.ArgumentMap(e.params.Variables)
	res := typ.Fields(args["includeDeprecated"].(bool))
	out := make([]any, len(res))
	for i, r := range res {
		out[i] = e.introspectField(&r, field.SelectionSet)
	}
	return out
}

func (e *executionContext) introspectTypes(obj []introspection.Type, sel ast.SelectionSet) []any {
	result := make([]any, len(obj))
	for i, t := range obj {
		result[i] = e.introspectType(&t, sel)
	}
	return result
}
