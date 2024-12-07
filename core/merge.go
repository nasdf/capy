package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// MergeBase returns the best common ancestor for merging the two given links.
func MergeBase(ctx context.Context, store *Store, oldLink, newLink datamodel.Link) ([]datamodel.Link, error) {
	newIter := store.ParentIterator(newLink)
	for !newIter.Done() {
		lnk, _, err := newIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if lnk == oldLink {
			return []datamodel.Link{lnk}, nil
		}
	}
	oldIter := store.ParentIterator(oldLink)
	for k, v := range newIter.seen {
		oldIter.seen[k] = v
	}
	var links []datamodel.Link
	for !oldIter.Done() {
		lnk, _, err := newIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		_, ok := newIter.seen[lnk.String()]
		if ok {
			links = append(links, lnk)
		}
	}
	return Independents(ctx, store, links)
}

// Independents returns a list links where each entry is not an ancestor of any other entry.
func Independents(ctx context.Context, store *Store, links []datamodel.Link) ([]datamodel.Link, error) {
	keep := make(map[string]struct{})
	seen := make(map[string]struct{})
	for _, l := range links {
		keep[l.String()] = struct{}{}
	}
	for _, l := range links {
		_, ok := keep[l.String()]
		if !ok {
			continue
		}
		iter := store.ParentIterator(l)
		iter.seen = seen
		for !iter.Done() {
			lnk, _, err := iter.Next(ctx)
			if err != nil {
				return nil, err
			}
			_, ok := keep[lnk.String()]
			if ok && l != lnk {
				delete(keep, lnk.String())
			}
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
func IsAncestor(ctx context.Context, store *Store, oldLink, newLink datamodel.Link) (bool, error) {
	iter := store.ParentIterator(newLink)
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
