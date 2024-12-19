//go:build js

package core_js

import (
	"context"
	"syscall/js"

	"github.com/nasdf/capy/core"
)

// storage wraps the JavaScript storage interface.
//
//	interface Storage {
//	  get(key: string): Promise<Uint8Array>
//	  set(key: string, val: Uint8Array): Promise<void>
//	}
type storage struct {
	impl js.Value
}

func NewStorage(value js.Value) core.Storage {
	return &storage{value}
}

func (s *storage) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := AwaitPromise(s.impl.Call("get", key))
	if err != nil {
		return nil, err
	}
	return BytesFromUint8Array(res[0]), nil
}

func (s *storage) Put(ctx context.Context, key string, value []byte) error {
	_, err := AwaitPromise(s.impl.Call("put", key, Uint8ArrayFromBytes(value)))
	return err
}
