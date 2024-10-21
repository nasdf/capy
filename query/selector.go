package query

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/vektah/gqlparser/v2/ast"
)

var ssb = builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

func querySelector(fields []graphql.CollectedField) builder.SelectorSpec {
	return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		for _, f := range fields {
			efsb.Insert(f.Name, ssb.ExploreAll(selectionSelector(f.Selections)))
		}
	})
}

func selectionSelector(selections ast.SelectionSet) builder.SelectorSpec {
	return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		for _, s := range selections {
			field := s.(*ast.Field)
			if len(field.SelectionSet) > 0 {
				efsb.Insert(field.Name, selectionSelector(field.SelectionSet))
			} else {
				efsb.Insert(field.Name, ssb.Matcher())
			}
		}
	})
}
