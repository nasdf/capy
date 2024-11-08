package node

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

// Build creates a node of the given type and value and returns its unique link.
func Build(ctx context.Context, store *core.Store, t schema.Type, value any) (datamodel.Link, error) {
	nb := bindnode.Prototype(nil, t).NewBuilder()
	if err := assignValue(ctx, store, t, value, nb); err != nil {
		return nil, err
	}
	return store.Store(ctx, nb.Build())
}

func assignValue(ctx context.Context, store *core.Store, t schema.Type, value any, na datamodel.NodeAssembler) error {
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
		return assignListValue(ctx, store, v, value.([]any), na)
	case *schema.TypeMap:
		return assignMapValue(ctx, store, v, value.(map[string]any), na)
	case *schema.TypeStruct:
		return assignStructValue(ctx, store, v, value.(map[string]any), na)
	case *schema.TypeLink:
		return assignLinkValue(ctx, store, v, value, na)
	default:
		return fmt.Errorf("unknown type %s", t.TypeKind().String())
	}
}

func assignListValue(ctx context.Context, store *core.Store, t *schema.TypeList, value []any, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err := assignValue(ctx, store, t.ValueType(), v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func assignMapValue(ctx context.Context, store *core.Store, t *schema.TypeMap, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = assignValue(ctx, store, t.ValueType(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func assignStructValue(ctx context.Context, store *core.Store, t *schema.TypeStruct, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = assignValue(ctx, store, t.Field(k).Type(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func assignLinkValue(ctx context.Context, store *core.Store, t *schema.TypeLink, value any, na datamodel.NodeAssembler) error {
	switch vt := value.(type) {
	case string:
		id, err := cid.Decode(vt)
		if err != nil {
			return err
		}
		return na.AssignLink(cidlink.Link{Cid: id})

	case map[string]any:
		if !t.HasReferencedType() {
			return fmt.Errorf("cannot create link of unknown reference type")
		}
		lnk, err := Build(ctx, store, t.ReferencedType(), value)
		if err != nil {
			return err
		}
		return na.AssignLink(lnk)

	default:
		return fmt.Errorf("invalid link value %v", value)
	}
}
