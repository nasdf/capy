package plan

import (
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var ssb = builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

// Request contains all of the request info.
type Request struct {
	// Fields is a list of all fields in the request.
	Fields []RequestField
}

func (r *Request) selectorSpec() builder.SelectorSpec {
	return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		for _, f := range r.Fields {
			efsb.Insert(f.Name, ssb.ExploreAll(f.selectorSpec()))
		}
	})
}

// RequestField contains info about a requested field.
type RequestField struct {
	// Name is the name of the field on the object.
	Name string
	// Alias is the name used to render the field.
	Alias string
	// Children is a list of child fields.
	Children []RequestField
	// Arguments contains optional arguments.
	Arguments map[string]any
}

func (r RequestField) selectorSpec() builder.SelectorSpec {
	if len(r.Children) == 0 {
		return ssb.Matcher()
	}
	return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		for _, c := range r.Children {
			efsb.Insert(c.Name, c.selectorSpec())
		}
	})
}
