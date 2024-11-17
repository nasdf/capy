package graphql

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func assignErrors(list gqlerror.List, na datamodel.MapAssembler) error {
	if len(list) == 0 {
		return nil
	}
	va, err := na.AssembleEntry("errors")
	if err != nil {
		return err
	}
	la, err := va.BeginList(int64(len(list)))
	if err != nil {
		return err
	}
	for _, e := range list {
		err = assignError(e, la.AssembleValue())
		if err != nil {
			return nil
		}
	}
	return la.Finish()
}

func assignError(e *gqlerror.Error, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(0)
	if err != nil {
		return err
	}
	err = assignErrorMessage(e.Message, ma)
	if err != nil {
		return err
	}
	err = assignErrorPath(e.Path, ma)
	if err != nil {
		return err
	}
	err = assignErrorLocations(e.Locations, ma)
	if err != nil {
		return err
	}
	return ma.Finish()
}

func assignErrorLocations(locations []gqlerror.Location, na datamodel.MapAssembler) error {
	if len(locations) == 0 {
		return nil
	}
	va, err := na.AssembleEntry("locations")
	if err != nil {
		return err
	}
	la, err := va.BeginList(int64(len(locations)))
	if err != nil {
		return err
	}
	for _, l := range locations {
		err = assignErrorLocation(l, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func assignErrorLocation(location gqlerror.Location, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(0)
	if err != nil {
		return err
	}
	if location.Column != 0 {
		va, err := ma.AssembleEntry("column")
		if err != nil {
			return err
		}
		err = va.AssignInt(int64(location.Column))
		if err != nil {
			return err
		}
	}
	if location.Line != 0 {
		va, err := ma.AssembleEntry("line")
		if err != nil {
			return err
		}
		err = va.AssignInt(int64(location.Line))
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func assignErrorPath(path ast.Path, na datamodel.MapAssembler) error {
	if len(path) == 0 {
		return nil
	}
	va, err := na.AssembleEntry("path")
	if err != nil {
		return err
	}
	la, err := va.BeginList(int64(len(path)))
	if err != nil {
		return err
	}
	for _, p := range path {
		switch t := p.(type) {
		case ast.PathIndex:
			err = la.AssembleValue().AssignInt(int64(int(t)))
			if err != nil {
				return err
			}

		case ast.PathName:
			err = la.AssembleValue().AssignString(string(t))
			if err != nil {
				return err
			}
		}
	}
	return la.Finish()
}

func assignErrorMessage(message string, na datamodel.MapAssembler) error {
	va, err := na.AssembleEntry("message")
	if err != nil {
		return err
	}
	return va.AssignString(message)
}
