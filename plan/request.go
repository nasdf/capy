package plan

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/node"
)

type Request struct {
	// Name is the name of the field on the object.
	Name string
	// Children is a list of child fields.
	Fields map[string]Request
	// Arguments contains optional arguments.
	Arguments map[string]any
}

func (r Request) matchFilters(node datamodel.Node) (bool, error) {
	// this is the only filter for now
	lnk, ok := r.Arguments["link"].(string)
	if !ok {
		return true, nil
	}
	other, err := node.AsLink()
	if err != nil {
		return false, err
	}
	return other.String() == lnk, nil
}

type Progress struct {
	Ctx   context.Context
	Link  datamodel.Link
	Store *data.Store
}

func (p Progress) Walk(n datamodel.Node, r Request) (any, error) {
	if len(r.Fields) == 0 {
		return node.Value(n)
	}
	switch n.Kind() {
	case datamodel.Kind_Link:
		return p.walkLink(n, r)
	case datamodel.Kind_List:
		return p.walkList(n, r)
	case datamodel.Kind_Map:
		return p.walkMap(n, r)
	case datamodel.Kind_Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot traverse node of type %s", n.Kind().String())
	}
}

func (p Progress) walkLink(n datamodel.Node, r Request) (any, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return nil, err
	}
	c, err := p.Store.Load(p.Ctx, lnk, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	p.Link = lnk
	return p.Walk(c, r)
}

func (p Progress) walkMap(n datamodel.Node, r Request) (map[string]any, error) {
	out := make(map[string]any)
	for alias, field := range r.Fields {
		// link references the currently loaded link
		if field.Name == "_link" {
			out[alias] = p.Link.String()
			continue
		}
		c, err := n.LookupByString(field.Name)
		if err != nil {
			return nil, err
		}
		v, err := p.Walk(c, field)
		if err != nil {
			return nil, err
		}
		out[alias] = v
	}
	return out, nil
}

func (p Progress) walkList(n datamodel.Node, r Request) ([]any, error) {
	list := make([]any, 0, n.Length())
	iter := n.ListIterator()
	for !iter.Done() {
		_, c, err := iter.Next()
		if err != nil {
			return nil, err
		}
		match, err := r.matchFilters(c)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		v, err := p.Walk(c, r)
		if err != nil {
			return nil, err
		}
		list = append(list, v)
	}
	return list, nil
}
