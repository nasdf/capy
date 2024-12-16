package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/introspection"
	"github.com/vektah/gqlparser/v2/ast"
)

func (e *Request) introspectSchema(obj *introspection.Schema, sel ast.SelectionSet) (any, error) {
	result := make(map[string]any)
	fields := e.collectFields(sel, "__Schema")
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Schema"
		case "types":
			res, err := e.introspectSchemaTypes(field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "queryType":
			res, err := e.introspectType(obj.QueryType(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "mutationType":
			res, err := e.introspectType(obj.MutationType(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "subscriptionType":
			res, err := e.introspectType(obj.SubscriptionType(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "directives":
			res, err := e.introspectDirectives(obj.Directives(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) introspectType(obj *introspection.Type, sel ast.SelectionSet) (any, error) {
	if obj == nil {
		return nil, nil
	}
	fields := e.collectFields(sel, "__Type")
	result := make(map[string]any, len(fields))
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
			res, err := e.introspectTypeFields(obj, field.Field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "interfaces":
			res, err := e.introspectTypes(obj.Interfaces(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "possibleTypes":
			res, err := e.introspectTypes(obj.PossibleTypes(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "enumValues":
			res, err := e.introspectTypeEnumValues(obj, field.Field)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "inputFields":
			res, err := e.introspectInputValues(obj.InputFields(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "ofType":
			res, err := e.introspectType(obj.OfType(), field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) introspectField(obj *introspection.Field, sel ast.SelectionSet) (any, error) {
	fields := e.collectFields(sel, "__Field")
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__Field"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "isDeprecated":
			result[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			result[field.Alias] = obj.DeprecationReason()
		case "args":
			res, err := e.introspectInputValues(obj.Args, field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		case "type":
			res, err := e.introspectType(obj.Type, field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) introspectInputValue(obj introspection.InputValue, sel ast.SelectionSet) (any, error) {
	fields := e.collectFields(sel, "__InputValue")
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			result[field.Alias] = "__InputValue"
		case "name":
			result[field.Alias] = obj.Name
		case "description":
			result[field.Alias] = obj.Description()
		case "defaultValue":
			result[field.Alias] = obj.DefaultValue
		case "type":
			res, err := e.introspectType(obj.Type, field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) introspectEnumValue(obj introspection.EnumValue, sel ast.SelectionSet) (any, error) {
	fields := e.collectFields(sel, "__EnumValue")
	result := make(map[string]any, len(fields))
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
	return result, nil
}

func (e *Request) introspectDirective(obj introspection.Directive, sel ast.SelectionSet) (any, error) {
	fields := e.collectFields(sel, "__Directive")
	result := make(map[string]any, len(fields))
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
			res, err := e.introspectInputValues(obj.Args, field.SelectionSet)
			if err != nil {
				return nil, err
			}
			result[field.Alias] = res
		}
	}
	return result, nil
}

func (e *Request) introspectDirectives(obj []introspection.Directive, sel ast.SelectionSet) (any, error) {
	result := make([]any, len(obj))
	for i, d := range obj {
		res, err := e.introspectDirective(d, sel)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}
	return result, nil
}

func (e *Request) introspectSchemaTypes(sel ast.SelectionSet) (any, error) {
	result := make([]any, 0, len(e.schema.Types))
	for _, t := range e.schema.Types {
		res, err := e.introspectType(introspection.WrapTypeFromDef(e.schema, t), sel)
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}

func (e *Request) introspectTypeEnumValues(obj *introspection.Type, field *ast.Field) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	vals := obj.EnumValues(args["includeDeprecated"].(bool))
	result := make([]any, len(vals))
	for i, v := range vals {
		res, err := e.introspectEnumValue(v, field.SelectionSet)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}
	return result, nil
}

func (e *Request) introspectInputValues(obj []introspection.InputValue, sel ast.SelectionSet) (any, error) {
	result := make([]any, len(obj))
	for i, o := range obj {
		res, err := e.introspectInputValue(o, sel)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}
	return result, nil
}

func (e *Request) introspectQuerySchema(field graphql.CollectedField) (any, error) {
	typ := introspection.WrapSchema(e.schema)
	return e.introspectSchema(typ, field.SelectionSet)
}

func (e *Request) introspectQueryType(field graphql.CollectedField) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	name := args["name"].(string)
	typ := introspection.WrapTypeFromDef(e.schema, e.schema.Types[name])
	return e.introspectType(typ, field.SelectionSet)
}

func (e *Request) introspectTypeFields(typ *introspection.Type, field *ast.Field) (any, error) {
	args := field.ArgumentMap(e.params.Variables)
	vals := typ.Fields(args["includeDeprecated"].(bool))
	result := make([]any, len(vals))
	for i, v := range vals {
		res, err := e.introspectField(&v, field.SelectionSet)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}
	return result, nil
}

func (e *Request) introspectTypes(obj []introspection.Type, sel ast.SelectionSet) (any, error) {
	result := make([]any, len(obj))
	for i, t := range obj {
		res, err := e.introspectType(&t, sel)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}
	return result, nil
}
