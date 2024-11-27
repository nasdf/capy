package graphql

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/core"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

func (e *executionContext) createDocument(ctx context.Context, collection string, value any) (string, error) {
	nt := e.tx.Type(collection)
	nb := bindnode.Prototype(nil, nt).NewBuilder()
	if err := e.assignValue(ctx, nt, value, nb); err != nil {
		return "", err
	}
	return e.tx.CreateDocument(ctx, collection, nb.Build())
}

func (e *executionContext) assignValue(ctx context.Context, t schema.Type, value any, na datamodel.NodeAssembler) error {
	collection, ok := core.RelationName(t)
	if ok {
		return e.assignReference(ctx, collection, value.(map[string]any), na)
	}
	switch v := t.(type) {
	case *schema.TypeBool:
		return na.AssignBool(value.(bool))
	case *schema.TypeString:
		return na.AssignString(value.(string))
	case *schema.TypeInt:
		return na.AssignInt(value.(int64))
	case *schema.TypeFloat:
		return na.AssignFloat(value.(float64))
	case *schema.TypeBytes:
		return na.AssignBytes(value.([]byte))
	case *schema.TypeList:
		return e.assignList(ctx, v, value.([]any), na)
	case *schema.TypeMap:
		return e.assignMap(ctx, v, value.(map[string]any), na)
	case *schema.TypeStruct:
		return e.assignStruct(ctx, v, value.(map[string]any), na)
	case *schema.TypeLink:
		return e.assignLink(value.(string), na)
	default:
		return fmt.Errorf("invalid type %s", t.TypeKind().String())
	}
}

func (e *executionContext) assignLink(value string, na datamodel.NodeAssembler) error {
	id, err := cid.Decode(value)
	if err != nil {
		return err
	}
	return na.AssignLink(cidlink.Link{Cid: id})
}

func (e *executionContext) assignReference(ctx context.Context, collection string, value map[string]any, na datamodel.NodeAssembler) error {
	// if the provided input contains an id use that instead
	id, ok := value["_id"].(string)
	if ok {
		return na.AssignString(id)
	}
	id, err := e.createDocument(ctx, collection, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (e *executionContext) assignList(ctx context.Context, t *schema.TypeList, value []any, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err := e.assignValue(ctx, t.ValueType(), v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) assignMap(ctx context.Context, t *schema.TypeMap, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = e.assignValue(ctx, t.ValueType(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func (e *executionContext) assignStruct(ctx context.Context, t *schema.TypeStruct, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = e.assignValue(ctx, t.Field(k).Type(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}
