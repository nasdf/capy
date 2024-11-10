package storage

import (
	"bytes"
	"context"
	"io"
)

type memory struct {
	values map[string][]byte
}

func NewMemory() Storage {
	return &memory{
		values: make(map[string][]byte),
	}
}

func (m *memory) Has(ctx context.Context, key string) (bool, error) {
	_, ok := m.values[key]
	return ok, nil
}

func (m *memory) Put(ctx context.Context, key string, content []byte) error {
	val := make([]byte, len(content))
	copy(val, content)
	m.values[key] = val
	return nil
}

func (m *memory) Get(ctx context.Context, key string) ([]byte, error) {
	content, ok := m.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	val := make([]byte, len(content))
	copy(val, content)
	return val, nil
}

func (m *memory) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	content, ok := m.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *memory) Peek(ctx context.Context, key string) ([]byte, io.Closer, error) {
	content, ok := m.values[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return content, io.NopCloser(nil), nil
}
