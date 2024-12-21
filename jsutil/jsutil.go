//go:build js

package jsutil

import (
	"errors"
	"sync"
	"syscall/js"
)

// NewError returns a new JavaScript error containing the message in the given error.
func NewError(err error) js.Value {
	return js.Global().Get("Error").New(err.Error())
}

// NewPromise returns a new promise that calls the given executor function.
func NewPromise(fn func(resolve, reject func(value js.Value) js.Value) any) js.Value {
	var executor js.Func
	executor = js.FuncOf(func(this js.Value, args []js.Value) any {
		executor.Release()
		resolve := func(value js.Value) js.Value {
			return args[0].Invoke(value)
		}
		reject := func(value js.Value) js.Value {
			return args[1].Invoke(value)
		}
		return fn(resolve, reject)
	})
	return js.Global().Get("Promise").New(executor)
}

// AwaitPromise is a helper function that waits for a promise to resolve or reject
// and returns the results and an error value.
func AwaitPromise(v js.Value) (res []js.Value, err error) {
	var wait sync.WaitGroup

	// called when the promise is resolved
	onFulfilled := js.FuncOf(func(this js.Value, args []js.Value) any {
		defer wait.Done()
		res = args
		return js.Undefined()
	})

	// called when the promise is rejected
	onRejected := js.FuncOf(func(this js.Value, args []js.Value) any {
		defer wait.Done()
		err = errors.New(args[0].String())
		return js.Undefined()
	})

	wait.Add(1)
	v.Call("then", onFulfilled, onRejected)
	wait.Wait()

	onFulfilled.Release()
	onRejected.Release()

	return
}

// Uint8ArrayFromBytes is a helper function that copies the given
// byte slice into a new Uint8Array.
func Uint8ArrayFromBytes(src []byte) js.Value {
	len := js.ValueOf(len(src))
	dst := js.Global().Get("Uint8Array").New(len)
	js.CopyBytesToJS(js.Value(dst), src)
	return dst
}

// BytesFromUint8Array is a helper function that copies the given
// Uint8Array into a new byte slice.
func BytesFromUint8Array(src js.Value) []byte {
	len := src.Length()
	dst := make([]byte, len)
	js.CopyBytesToGo(dst, js.Value(src))
	return dst
}
