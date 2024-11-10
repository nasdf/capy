package node

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// Value returns the go value for the given node.
func Value(n datamodel.Node) (any, error) {
	switch n.Kind() {
	case datamodel.Kind_Bool:
		return n.AsBool()
	case datamodel.Kind_Bytes:
		return n.AsBytes()
	case datamodel.Kind_Float:
		return n.AsFloat()
	case datamodel.Kind_Int:
		return n.AsInt()
	case datamodel.Kind_String:
		return n.AsString()
	case datamodel.Kind_List:
		return ListValue(n)
	case datamodel.Kind_Map:
		return MapValue(n)
	case datamodel.Kind_Null:
		return nil, nil
	case datamodel.Kind_Link:
		lnk, err := n.AsLink()
		if err != nil {
			return nil, err
		}
		return lnk.String(), nil
	default:
		return nil, fmt.Errorf("cannot get value from %s", n.Kind().String())
	}
}

// MapValue returns a go map containing the values in the given node.
func MapValue(n datamodel.Node) (map[string]any, error) {
	out := make(map[string]any)
	for iter := n.MapIterator(); !iter.Done(); {
		k, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		key, err := Value(k)
		if err != nil {
			return nil, err
		}
		val, err := Value(v)
		if err != nil {
			return nil, err
		}
		out[key.(string)] = val
	}
	return out, nil
}

// ListValue returns a go slice containing the values in the given node.
func ListValue(n datamodel.Node) ([]any, error) {
	out := make([]any, n.Length())
	for iter := n.ListIterator(); !iter.Done(); {
		i, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := Value(v)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}
