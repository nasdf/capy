package capy

import (
	"context"
	"os"
	"testing"

	"github.com/nasdf/capy/query"

	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/stretchr/testify/require"
)

var testSchema = `type User {
	Name: String
	Stuff: [String]
}`

var testQuery = `mutation {
	createUser(input: {Name: "Bob", Stuff: ["one", "two"]}) {
		Name
		Stuff
	}
}`

func TestBasicQuery(t *testing.T) {
	ctx := context.Background()

	db, err := New(ctx, testSchema)
	require.NoError(t, err)

	res, err := db.Execute(ctx, &query.Params{
		Query: testQuery,
	})
	require.NoError(t, err)

	err = dagjson.Encode(res, os.Stdout)
	require.NoError(t, err)
}
