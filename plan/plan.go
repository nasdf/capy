package plan

import (
	"context"

	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/node"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// Node represents an operation to perform on an IPLD graph.
type Node interface {
	// Execute returns the results after running the Node operations.
	Execute(ctx context.Context, p *Planner) (*Result, error)
}

type Planner struct {
	store   data.Store
	typeSys schema.TypeSystem
	rootLnk datamodel.Link
}

func NewPlanner(store data.Store, typeSys schema.TypeSystem, rootLnk datamodel.Link) *Planner {
	return &Planner{
		store:   store,
		typeSys: typeSys,
		rootLnk: rootLnk,
	}
}

func (p *Planner) Execute(ctx context.Context, node Node) (datamodel.Link, *Result, error) {
	res, err := node.Execute(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	return p.rootLnk, res, nil
}

func (p *Planner) query(ctx context.Context, req Request) (*Result, error) {
	rootType := p.typeSys.TypeByName(data.RootTypeName)
	rootNode, err := p.store.Load(ctx, p.rootLnk, bindnode.Prototype(nil, rootType))
	if err != nil {
		return nil, err
	}
	sel, err := req.selectorSpec().Selector()
	if err != nil {
		return nil, err
	}
	res := NewResult()
	err = p.store.Traversal(ctx).WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		return res.Set(p.Path, n)
	})
	return res, err
}

func (p *Planner) create(ctx context.Context, collection string, value any) (datamodel.Link, error) {
	builder := node.NewBuilder(p.store)
	lnk, err := builder.Build(ctx, p.typeSys.TypeByName(collection), value)
	if err != nil {
		return nil, err
	}
	rootType := bindnode.Prototype(nil, p.typeSys.TypeByName(data.RootTypeName))
	rootNode, err := p.store.Load(ctx, p.rootLnk, rootType)
	if err != nil {
		return nil, err
	}
	// append all of the objects that were created
	for col, links := range builder.Links() {
		for _, lnk := range links {
			path := datamodel.ParsePath(col).AppendSegmentString("-")
			rootNode, err = p.store.Traversal(ctx).FocusedTransform(rootNode, path, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
				return basicnode.NewLink(lnk), nil
			}, true)
			if err != nil {
				return nil, err
			}
		}
	}
	rootLnk, err := p.store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	p.rootLnk = rootLnk
	return lnk, nil
}

func (p *Planner) findIndex(ctx context.Context, collection string, lnk datamodel.Link) (int64, error) {
	rootType := bindnode.Prototype(nil, p.typeSys.TypeByName(data.RootTypeName))
	rootNode, err := p.store.Load(ctx, p.rootLnk, rootType)
	if err != nil {
		return -1, err
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel, err := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert(collection, ssb.ExploreAll(ssb.Matcher()))
	}).Selector()
	if err != nil {
		return -1, err
	}
	index := int64(-1)
	return index, p.store.Traversal(ctx).WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		if p.LastBlock.Link.String() != lnk.String() {
			return nil
		}
		index, err = p.Path.Last().Index()
		return err
	})
}
