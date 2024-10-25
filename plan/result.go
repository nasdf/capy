package plan

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/nasdf/capy/node"
)

var _ json.Marshaler = (*Result)(nil)

// Result contains the results of a traversal.
type Result struct {
	results any
}

// NewResult returns a new empty Result.
func NewResult() *Result {
	return &Result{}
}

func (r *Result) MarshalJSON() ([]byte, error) {
	switch t := r.results.(type) {
	case map[string]any:
		return json.Marshal(t)

	case map[int64]any:
		vals := make([]any, 0, len(t))
		keys := make([]int64, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			vals = append(vals, t[k])
		}
		return json.Marshal(vals)

	default:
		return json.Marshal(t)
	}
}

// Set sets the value of the result at the given path.
func (r *Result) Set(path datamodel.Path, n datamodel.Node) error {
	if path.Len() == 0 {
		return nil
	}
	s, p := path.Shift()
	// if the segment is a valid index then the sub object is an array
	if i, err := s.Index(); err == nil {
		return r.setListEntry(i, p, n)
	}
	return r.setObjectProp(s.String(), p, n)
}

func (r *Result) setObjectProp(key string, path datamodel.Path, n datamodel.Node) error {
	if r.results == nil {
		r.results = make(map[string]any)
	}
	res, ok := r.results.(map[string]any)
	if !ok {
		return fmt.Errorf("expected an object")
	}
	switch {
	case path.Len() == 0:
		val, err := node.Value(n)
		if err != nil {
			return err
		}
		res[key] = val
		return nil

	default:
		if _, ok := res[key]; !ok {
			res[key] = &Result{}
		}
		sub, ok := res[key].(*Result)
		if !ok {
			return fmt.Errorf("expected a mapper")
		}
		return sub.Set(path, n)
	}
}

func (r *Result) setListEntry(index int64, path datamodel.Path, n datamodel.Node) error {
	if r.results == nil {
		r.results = make(map[int64]any, 0)
	}
	res, ok := r.results.(map[int64]any)
	if !ok {
		return fmt.Errorf("expected a list")
	}
	switch {
	case path.Len() == 0:
		val, err := node.Value(n)
		if err != nil {
			return err
		}
		res[index] = val
		return nil

	default:
		if _, ok := res[index]; !ok {
			res[index] = &Result{}
		}
		sub, ok := res[index].(*Result)
		if !ok {
			return fmt.Errorf("expected a mapper")
		}
		return sub.Set(path, n)
	}
}
