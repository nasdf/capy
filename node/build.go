package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/types"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

// GenerateIdFunc is used to generate unique IDs for documents.
type GenerateIdFunc = func() (string, error)

// defaultGenerateIdFunc is used to generate Ids when none is provided.
var defaultGenerateIdFunc = func() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Builder assembles nodes from go input values.
type Builder struct {
	store  *core.Store
	system *types.System
	links  map[string]map[string]datamodel.Link
	genId  GenerateIdFunc
}

// NewBuilder returns a new builder that uses the given type system to create nodes.
func NewBuilder(store *core.Store, system *types.System) *Builder {
	return &Builder{
		store:  store,
		system: system,
		links:  make(map[string]map[string]datamodel.Link),
		genId:  defaultGenerateIdFunc,
	}
}

// Links returns a mapping of collection names to links that were created from building nodes.
func (b *Builder) Links() map[string]map[string]datamodel.Link {
	return b.links
}

// Build creates a new node using the provided collection type and value returning its unique ID.
func (b *Builder) Build(ctx context.Context, collection string, value any) (string, error) {
	nt := b.system.Type(collection)
	nb := bindnode.Prototype(nil, nt).NewBuilder()
	if err := b.assignValue(ctx, nt, value, nb); err != nil {
		return "", err
	}
	id, err := b.genId()
	if err != nil {
		return "", err
	}
	lnk, err := b.store.Store(ctx, nb.Build())
	if err != nil {
		return "", err
	}
	if _, ok := b.links[collection]; !ok {
		b.links[collection] = make(map[string]datamodel.Link)
	}
	b.links[collection][id] = lnk
	return id, nil
}

func (b *Builder) assignValue(ctx context.Context, t schema.Type, value any, na datamodel.NodeAssembler) error {
	// check if the type is a document id
	if _, ok := IsDocumentID(t); ok {
		return b.assignReference(ctx, t, value, na)
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
		return fmt.Errorf("unknown type %s", t.TypeKind().String())
	}
}

func (b *Builder) assignLink(value string, na datamodel.NodeAssembler) error {
	id, err := cid.Decode(value)
	if err != nil {
		return err
	}
	return na.AssignLink(cidlink.Link{Cid: id})
}

func (b *Builder) assignReference(ctx context.Context, t schema.Type, value any, na datamodel.NodeAssembler) error {
	collection := strings.TrimSuffix(t.Name(), types.IDSuffix)
	id, err := b.Build(ctx, collection, value)
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
