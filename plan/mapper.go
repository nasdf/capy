package plan

import "github.com/ipld/go-ipld-prime/datamodel"

// Mapper maps query node paths into result node paths.
type Mapper struct {
	mapping map[string]datamodel.PathSegment
}

func NewMapper() *Mapper {
	return &Mapper{
		mapping: make(map[string]datamodel.PathSegment),
	}
}

// Path returns a remapped path from the given path.
func (r *Mapper) Path(p datamodel.Path) datamodel.Path {
	segments := p.Segments()
	if len(segments) < 2 {
		return p
	}
	index := segments[1].String()
	remap, ok := r.mapping[index]
	if !ok {
		// the first remap returns an append operation
		remap = datamodel.PathSegmentOfString("-")
		r.mapping[index] = datamodel.PathSegmentOfInt(int64(len(r.mapping)))
	}
	segments[1] = remap
	return datamodel.NewPath(segments)
}
