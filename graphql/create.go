package graphql

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
)

func (e *executionContext) createDocument(ctx context.Context, collection string, value map[string]any) (string, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(int64(len(value)))
	if err != nil {
		return "", err
	}
	def, ok := e.schema.Types[collection]
	if !ok {
		return "", fmt.Errorf("invalid document type %s", collection)
	}
	for k, v := range value {
		field := def.Fields.ForName(k)
		if field == nil {
			return "", fmt.Errorf("invalid document field %s", k)
		}
		na, err := ma.AssembleEntry(field.Name)
		if err != nil {
			return "", err
		}
		err = e.assignValue(ctx, field.Type, v, na)
		if err != nil {
			return "", err
		}
	}
	err = ma.Finish()
	if err != nil {
		return "", err
	}
	return e.tx.CreateDocument(ctx, collection, nb.Build())
}

func (e *executionContext) assignValue(ctx context.Context, typ *ast.Type, value any, na datamodel.NodeAssembler) error {
	if !typ.NonNull && value == nil {
		return na.AssignNull()
	}
	if typ.Elem != nil {
		return e.assignList(ctx, typ.Elem, value.([]any), na)
	}
	def := e.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return e.assignRelation(ctx, typ, value.(map[string]any), na)
	}
	switch typ.NamedType {
	case "String":
		return na.AssignString(value.(string))
	case "Boolean":
		return na.AssignBool(value.(bool))
	case "Int":
		return na.AssignInt(value.(int64))
	case "Float":
		return na.AssignFloat(value.(float64))
	default:
		return fmt.Errorf("invalid type %s", typ.NamedType)
	}
}

func (e *executionContext) assignList(ctx context.Context, typ *ast.Type, value []any, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err = e.assignValue(ctx, typ, v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) assignRelation(ctx context.Context, typ *ast.Type, value map[string]any, na datamodel.NodeAssembler) error {
	id, ok := value["_id"].(string)
	if ok {
		return na.AssignString(id)
	}
	id, err := e.createDocument(ctx, typ.NamedType, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}
