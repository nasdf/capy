//go:build js

package core

import (
	"context"
	"syscall/js"

	"github.com/nasdf/capy/jsutil"
)

// jsStorage wraps the JavaScript Storage interface.
//
//	interface Storage {
//	  get(key: string): Promise<Uint8Array>
//	  set(key: string, val: Uint8Array): Promise<void>
//	}
type jsStorage js.Value

// NewJSStorage returns Storage that is backed by a JavaScript implementation.
func NewJSStorage(v js.Value) Storage {
	return jsStorage(v)
}

func (s jsStorage) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := jsutil.AwaitPromise(js.Value(s).Call("get", key))
	if err != nil {
		return nil, err
	}
	return jsutil.BytesFromUint8Array(res[0]), nil
}

func (s jsStorage) Put(ctx context.Context, key string, value []byte) error {
	_, err := jsutil.AwaitPromise(js.Value(s).Call("put", key, jsutil.Uint8ArrayFromBytes(value)))
	return err
}
