package core

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/graphql/schema_gen"
	"github.com/nasdf/capy/link"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
)

type Store struct {
	links    *link.Store
	schema   *ast.Schema
	rootLink datamodel.Link
}

func NewStore(ctx context.Context, links *link.Store, rootLink datamodel.Link) (*Store, error) {
	rootNode, err := links.Load(ctx, rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	schemaPath := datamodel.ParsePath(RootSchemaFieldName)
	schemaNode, err := links.GetNode(ctx, schemaPath, rootNode)
	if err != nil {
		return nil, err
	}
	schemaInput, err := schemaNode.AsString()
	if err != nil {
		return nil, err
	}
	schema, err := schema_gen.Execute(schemaInput)
	if err != nil {
		return nil, err
	}
	return &Store{
		links:    links,
		schema:   schema,
		rootLink: rootLink,
	}, nil
}

func (s *Store) RootLink() datamodel.Link {
	return s.rootLink
}

func (s *Store) Merge(ctx context.Context, rootLink datamodel.Link) error {
	bases, err := s.MergeBase(ctx, s.rootLink, rootLink)
	if err != nil {
		return err
	}
	if len(bases) == 0 {
		return fmt.Errorf("no merge base found")
	}
	switch bases[0] {
	case s.rootLink:
		// fast-forward merge
		s.rootLink = rootLink
		return nil

	case rootLink:
		// nothing to merge
		return nil

	default:
		return fmt.Errorf("merge not implemented")
	}
}

// MergeBase returns the best common ancestor for merging the two given links.
func (s *Store) MergeBase(ctx context.Context, oldLink, newLink datamodel.Link) ([]datamodel.Link, error) {
	var links []datamodel.Link
	seen := map[string]struct{}{}

	newIter := s.ParentIterator(newLink)
	oldIter := s.ParentIterator(oldLink)

	for !newIter.Done() {
		lnk, _, err := newIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if lnk == oldLink {
			return []datamodel.Link{lnk}, nil
		}
		seen[lnk.String()] = struct{}{}
	}

	for !oldIter.Done() {
		lnk, _, err := oldIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		_, ok := seen[lnk.String()]
		if !ok {
			continue
		}
		links = append(links, lnk)
		oldIter.Skip()
	}

	return s.Independents(ctx, links)
}

// Independents returns a sub list where each entry is not an ancestor of any other entry.
func (s *Store) Independents(ctx context.Context, links []datamodel.Link) ([]datamodel.Link, error) {
	if len(links) < 2 {
		return links, nil
	}

	seen := make(map[string]struct{})
	keep := make(map[string]struct{})

	for _, l := range links {
		keep[l.String()] = struct{}{}
	}

	for _, l := range links {
		_, ok := keep[l.String()]
		if !ok {
			continue
		}
		iter := s.ParentIterator(l)
		for !iter.Done() {
			lnk, _, err := iter.Next(ctx)
			if err != nil {
				return nil, err
			}
			_, ok := keep[lnk.String()]
			if ok && l != lnk {
				delete(keep, lnk.String())
			}
			_, ok = seen[lnk.String()]
			if ok {
				iter.Skip()
			}
			seen[lnk.String()] = struct{}{}
		}
	}

	result := make([]datamodel.Link, 0, len(keep))
	for _, l := range links {
		_, ok := keep[l.String()]
		if ok {
			result = append(result, l)
		}
	}

	return result, nil
}

// IsAncestor returns true if the old link is an ancestor of the new link.
func (s *Store) IsAncestor(ctx context.Context, oldLink, newLink datamodel.Link) (bool, error) {
	iter := s.ParentIterator(newLink)
	for !iter.Done() {
		lnk, _, err := iter.Next(ctx)
		if err != nil {
			return false, err
		}
		if lnk == oldLink {
			return true, nil
		}
	}
	return false, nil
}

// Dump returns a map of collections to document ids.
//
// This function is primarily used for testing.
func (s *Store) Dump(ctx context.Context) (map[string][]string, error) {
	rootNode, err := s.links.Load(ctx, s.rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	collectionsLinkNode, err := rootNode.LookupByString(RootCollectionsFieldName)
	if err != nil {
		return nil, err
	}
	collectionsLink, err := collectionsLinkNode.AsLink()
	if err != nil {
		return nil, err
	}
	collectionsNode, err := s.links.Load(ctx, collectionsLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	docs := make(map[string][]string)
	iter := collectionsNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		collection, err := k.AsString()
		if err != nil {
			return nil, err
		}
		collectionLink, err := v.AsLink()
		if err != nil {
			return nil, err
		}
		collectionNode, err := s.links.Load(ctx, collectionLink, basicnode.Prototype.Map)
		if err != nil {
			return nil, err
		}
		documentsNode, err := collectionNode.LookupByString(CollectionDocumentsFieldName)
		if err != nil {
			return nil, err
		}
		documentIter := documentsNode.MapIterator()
		for !documentIter.Done() {
			k, _, err := documentIter.Next()
			if err != nil {
				return nil, err
			}
			id, err := k.AsString()
			if err != nil {
				return nil, err
			}
			docs[collection] = append(docs[collection], id)
		}
	}
	return docs, nil
}
