package node

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

// Store is the minimal store interface for the node builder.
type Store interface {
	Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error)
}

// Builder creates nodes from go values.
type Builder struct {
	store Store
	links map[string][]datamodel.Link
}

// NewBuilder returns a new empty builder that saves nodes in the given store.
func NewBuilder(store Store) *Builder {
	return &Builder{
		store: store,
		links: make(map[string][]datamodel.Link),
	}
}

// Links returns a mapping of type names to links that have been created by this builder.
func (b *Builder) Links() map[string][]datamodel.Link {
	return b.links
}

// Build returns a new node of the given type by parsing the value.
func (b *Builder) Build(ctx context.Context, t schema.Type, value any) (datamodel.Link, error) {
	nb := bindnode.Prototype(nil, t).NewBuilder()
	if err := b.assignValue(ctx, t, value, nb); err != nil {
		return nil, err
	}
	lnk, err := b.store.Store(ctx, nb.Build())
	if err != nil {
		return nil, err
	}
	b.links[t.Name()] = append(b.links[t.Name()], lnk)
	return lnk, nil
}

func (b *Builder) assignValue(ctx context.Context, t schema.Type, value any, na datamodel.NodeAssembler) error {
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
		return b.assignListValue(ctx, v, value.([]any), na)
	case *schema.TypeMap:
		return b.assignMapValue(ctx, v, value.(map[string]any), na)
	case *schema.TypeStruct:
		return b.assignStructValue(ctx, v, value.(map[string]any), na)
	case *schema.TypeLink:
		return b.assignLinkValue(ctx, v, value, na)
	default:
		return fmt.Errorf("unknown type %s", t.TypeKind().String())
	}
}

func (b *Builder) assignListValue(ctx context.Context, t *schema.TypeList, value []any, na datamodel.NodeAssembler) error {
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

func (b *Builder) assignMapValue(ctx context.Context, t *schema.TypeMap, value map[string]any, na datamodel.NodeAssembler) error {
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

func (b *Builder) assignStructValue(ctx context.Context, t *schema.TypeStruct, value map[string]any, na datamodel.NodeAssembler) error {
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

func (b *Builder) assignLinkValue(ctx context.Context, t *schema.TypeLink, value any, na datamodel.NodeAssembler) error {
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
		lnk, err := b.Build(ctx, t.ReferencedType(), value)
		if err != nil {
			return err
		}
		return na.AssignLink(lnk)

	default:
		return fmt.Errorf("invalid link value %v", value)
	}
}
