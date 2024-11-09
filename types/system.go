package types

import (
	"errors"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

type System struct {
	schema      string
	system      *ipldschema.TypeSystem
	collections []string
}

func NewSystem(schema string) (*System, error) {
	s, err := gqlparser.LoadSchema(&ast.Source{Input: schema})
	if err != nil {
		return nil, err
	}
	collections := []string{}
	for _, d := range s.Types {
		if !d.BuiltIn && d.Kind == ast.Object {
			collections = append(collections, d.Name)
		}
	}
	system := accumulate(s, collections)
	errs := system.ValidateGraph()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return &System{
		schema:      schema,
		system:      system,
		collections: collections,
	}, nil
}

func (s System) Type(name string) schema.Type {
	return s.system.TypeByName(name)
}

func (s System) Prototype(name string) datamodel.NodePrototype {
	return bindnode.Prototype(nil, s.Type(name))
}

func (s System) Collections() []string {
	return s.collections
}

// RootNode returns a new empty root node.
func (s System) RootNode() (datamodel.Node, error) {
	nb := s.Prototype(RootTypeName).NewBuilder()
	mb, err := nb.BeginMap(1)
	if err != nil {
		return nil, err
	}
	na, err := mb.AssembleEntry(RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	err = na.AssignString(s.schema)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
