package capy

import (
	"context"

	"github.com/nasdf/capy/core"
)

func Open(ctx context.Context, storage core.Storage) (*core.Repository, error) {
	return core.OpenRepository(ctx, storage)
}

func Init(ctx context.Context, storage core.Storage, schemaSource string) (*core.Repository, error) {
	return core.InitRepository(ctx, storage, schemaSource)
}
