package plan

import "github.com/ipld/go-ipld-prime/datamodel"

// Mapper maps query node paths into result node paths.
//
// This happens because a graph traversal can return array indices
// that are outside of the bounds of the result array.
type Mapper struct {
	counter map[string]int64
	mapping map[string]datamodel.PathSegment
}

// NewMapper returns a new empty Mapper.
func NewMapper() *Mapper {
	return &Mapper{
		counter: make(map[string]int64),
		mapping: make(map[string]datamodel.PathSegment),
	}
}

// Path returns a remapped path from the given path.
func (r *Mapper) Path(p datamodel.Path) datamodel.Path {
	segments := p.Segments()
	for i, s := range segments {
		if _, err := s.Index(); err != nil {
			continue
		}
		path := datamodel.NewPath(segments[:i])
		remap, ok := r.mapping[path.String()]
		if !ok {
			// the first remap returns an append operation
			remap = datamodel.PathSegmentOfString("-")
			count := r.counter[path.String()]
			r.counter[path.String()] = count + 1
			r.mapping[path.String()] = datamodel.PathSegmentOfInt(count)
		}
		segments[i] = remap
	}
	return datamodel.NewPath(segments)
}
