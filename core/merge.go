package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/nasdf/capy/link"
)

// MergeConflictResolver is a callback function that is used to resolver merge conflicts.
type MergeConflictResolver func(base, ours, theirs datamodel.Node) (datamodel.Node, error)

// TheirsConflictResolver is a merge strategy that favors the changes labeled as theirs.
var TheirsConflictResolver MergeConflictResolver = func(base, ours, theirs datamodel.Node) (datamodel.Node, error) {
	return theirs, nil
}

// OursConflictResolver is a merge strategy that favors the changes labeled as ours.
var OursConflictResolver MergeConflictResolver = func(base, ours, theirs datamodel.Node) (datamodel.Node, error) {
	return ours, nil
}

func (s *Store) mergeRoot(ctx context.Context, base, ours, theirs datamodel.Link) (datamodel.Link, error) {
	baseNode, err := s.links.Load(ctx, base, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	ourNode, err := s.links.Load(ctx, ours, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	theirNode, err := s.links.Load(ctx, theirs, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}

	baseCols, err := baseNode.LookupByString(RootCollectionsFieldName)
	if err != nil {
		return nil, err
	}
	ourCols, err := ourNode.LookupByString(RootCollectionsFieldName)
	if err != nil {
		return nil, err
	}
	theirCols, err := theirNode.LookupByString(RootCollectionsFieldName)
	if err != nil {
		return nil, err
	}

	nb := basicnode.Prototype.Any.NewBuilder()
	err = s.mergeNode(ctx, baseCols, ourCols, theirCols, nb)
	if err != nil {
		return nil, err
	}
	schemaNode, err := ourNode.LookupByString(RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	schemaLink, err := schemaNode.AsLink()
	if err != nil {
		return nil, err
	}
	collectionsLink, err := s.links.Store(ctx, nb.Build())
	if err != nil {
		return nil, err
	}
	parentsNode, err := BuildRootParentsNode(ours, theirs)
	if err != nil {
		return nil, err
	}
	rootNode, err := BuildRootNode(ctx, schemaLink, collectionsLink, parentsNode)
	if err != nil {
		return nil, err
	}
	return s.links.Store(ctx, rootNode)
}

func (s *Store) mergeNode(ctx context.Context, base, ours, theirs datamodel.Node, na datamodel.NodeAssembler) error {
	oursEqual := datamodel.DeepEqual(base, ours)
	theirsEqual := datamodel.DeepEqual(base, theirs)
	if base != nil && base.Kind() == datamodel.Kind_Link && !(oursEqual && theirsEqual) {
		return s.mergeLink(ctx, base, ours, theirs, na)
	}
	if base != nil && base.Kind() == datamodel.Kind_Map && !(oursEqual && theirsEqual) {
		return s.mergeMap(ctx, base, ours, theirs, na)
	}
	switch {
	case !oursEqual && !theirsEqual:
		res, err := s.resolver(base, ours, theirs)
		if err != nil {
			return err
		}
		if res == nil {
			return na.AssignNull()
		}
		return na.AssignNode(res)
	case !oursEqual:
		if ours == nil {
			return na.AssignNull()
		}
		return na.AssignNode(ours)
	case !theirsEqual:
		if theirs == nil {
			return na.AssignNull()
		}
		return na.AssignNode(theirs)
	default:
		if base == nil {
			return na.AssignNull()
		}
		return na.AssignNode(base)
	}
}

func (s *Store) mergeMap(ctx context.Context, base, ours, theirs datamodel.Node, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(base.Length())
	if err != nil {
		return err
	}
	seen := make(map[string]struct{})
	iter := tryMapIterator(base)
	for iter != nil && !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		prop, err := k.AsString()
		if err != nil {
			return err
		}
		ea, err := ma.AssembleEntry(prop)
		if err != nil {
			return err
		}
		ourNode, err := tryLookupByString(ours, prop)
		if err != nil {
			return err
		}
		theirNode, err := tryLookupByString(theirs, prop)
		if err != nil {
			return err
		}
		err = s.mergeNode(ctx, v, ourNode, theirNode, ea)
		if err != nil {
			return err
		}
		seen[prop] = struct{}{}
	}
	iter = tryMapIterator(ours)
	for iter != nil && !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		prop, err := k.AsString()
		if err != nil {
			return err
		}
		_, ok := seen[prop]
		if ok {
			continue
		}
		ea, err := ma.AssembleEntry(prop)
		if err != nil {
			return err
		}
		baseNode, err := tryLookupByString(base, prop)
		if err != nil {
			return err
		}
		theirNode, err := tryLookupByString(theirs, prop)
		if err != nil {
			return err
		}
		err = s.mergeNode(ctx, baseNode, v, theirNode, ea)
		if err != nil {
			return err
		}
		seen[prop] = struct{}{}
	}
	iter = tryMapIterator(theirs)
	for iter != nil && !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		prop, err := k.AsString()
		if err != nil {
			return err
		}
		_, ok := seen[prop]
		if ok {
			continue
		}
		ea, err := ma.AssembleEntry(prop)
		if err != nil {
			return err
		}
		ourNode, err := tryLookupByString(ours, prop)
		if err != nil {
			return err
		}
		baseNode, err := tryLookupByString(base, prop)
		if err != nil {
			return err
		}
		err = s.mergeNode(ctx, baseNode, ourNode, v, ea)
		if err != nil {
			return err
		}
		seen[prop] = struct{}{}
	}
	return ma.Finish()
}

func (s *Store) mergeLink(ctx context.Context, base, ours, theirs datamodel.Node, na datamodel.NodeAssembler) error {
	baseNode, err := tryLoadLink(ctx, s.links, base)
	if err != nil {
		return err
	}
	ourNode, err := tryLoadLink(ctx, s.links, ours)
	if err != nil {
		return err
	}
	theirNode, err := tryLoadLink(ctx, s.links, theirs)
	if err != nil {
		return err
	}

	nb := basicnode.Prototype.Any.NewBuilder()
	err = s.mergeNode(ctx, baseNode, ourNode, theirNode, nb)
	if err != nil {
		return err
	}
	lnk, err := s.links.Store(ctx, nb.Build())
	if err != nil {
		return err
	}
	return na.AssignLink(lnk)
}

func tryMapIterator(node datamodel.Node) datamodel.MapIterator {
	if node == nil {
		return nil
	}
	return node.MapIterator()
}

func tryLookupByString(node datamodel.Node, prop string) (datamodel.Node, error) {
	if node == nil {
		return nil, nil
	}
	n, err := node.LookupByString(prop)
	if _, ok := err.(datamodel.ErrNotExists); !ok && err != nil {
		return nil, err
	}
	return n, nil
}

func tryLoadLink(ctx context.Context, links *link.Store, node datamodel.Node) (datamodel.Node, error) {
	if node == nil {
		return nil, nil
	}
	link, err := node.AsLink()
	if err != nil {
		return nil, err
	}
	return links.Load(ctx, link, basicnode.Prototype.Any)
}
