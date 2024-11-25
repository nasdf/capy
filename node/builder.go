package node

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/types"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

// Builder assembles nodes from go input values.
type Builder struct {
	store     *core.Store
	system    *types.System
	documents map[string]datamodel.Link
}

// NewBuilder returns a new builder that uses the given type system to create nodes.
func NewBuilder(store *core.Store, system *types.System) *Builder {
	return &Builder{
		store:     store,
		system:    system,
		documents: make(map[string]datamodel.Link),
	}
}

// Documents returns a mapping of paths to document links that were created from building nodes.
func (b *Builder) Documents() map[string]datamodel.Link {
	return b.documents
}

// Build creates a new node using the provided collection type and value returning its unique ID.
func (b *Builder) Build(ctx context.Context, collection string, value any) (string, error) {
	nt := b.system.Type(collection + types.DocumentSuffix)
	nb := bindnode.Prototype(nil, nt).NewBuilder()
	if err := b.assignValue(ctx, nt, value, nb); err != nil {
		return "", err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	lnk, err := b.store.Store(ctx, nb.Build())
	if err != nil {
		return "", err
	}
	b.documents[collection+"/"+id.String()] = lnk
	return id.String(), nil
}

func (b *Builder) assignValue(ctx context.Context, t schema.Type, value any, na datamodel.NodeAssembler) error {
	if b.system.IsRelation(t) {
		return b.assignReference(ctx, t, value.(map[string]any), na)
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
		return b.assignList(ctx, v, value.([]any), na)
	case *schema.TypeMap:
		return b.assignMap(ctx, v, value.(map[string]any), na)
	case *schema.TypeStruct:
		return b.assignStruct(ctx, v, value.(map[string]any), na)
	case *schema.TypeLink:
		return b.assignLink(value.(string), na)
	default:
		return fmt.Errorf("invalid type %s", t.TypeKind().String())
	}
}

func (b *Builder) assignLink(value string, na datamodel.NodeAssembler) error {
	id, err := cid.Decode(value)
	if err != nil {
		return err
	}
	return na.AssignLink(cidlink.Link{Cid: id})
}

func (b *Builder) assignReference(ctx context.Context, t schema.Type, value map[string]any, na datamodel.NodeAssembler) error {
	// if the provided input contains an id use that instead
	id, ok := value["_id"].(string)
	if ok {
		return na.AssignString(id)
	}
	id, err := b.Build(ctx, t.Name(), value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (b *Builder) assignList(ctx context.Context, t *schema.TypeList, value []any, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err := b.assignValue(ctx, t.ValueType(), v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (b *Builder) assignMap(ctx context.Context, t *schema.TypeMap, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = b.assignValue(ctx, t.ValueType(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func (b *Builder) assignStruct(ctx context.Context, t *schema.TypeStruct, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		ea, err := ma.AssembleEntry(k)
		if err != nil {
			return err
		}
		err = b.assignValue(ctx, t.Field(k).Type(), v, ea)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}
