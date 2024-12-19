package core

import "context"

type memoryStorage struct {
	values map[string][]byte
}

func NewMemoryStorage() Storage {
	return &memoryStorage{
		values: make(map[string][]byte),
	}
}

func (m *memoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	content, ok := m.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	val := make([]byte, len(content))
	copy(val, content)
	return val, nil
}

func (m *memoryStorage) Put(ctx context.Context, key string, value []byte) error {
	val := make([]byte, len(value))
	copy(val, value)
	m.values[key] = val
	return nil
}
