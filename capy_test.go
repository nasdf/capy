package capy

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/graphql"
	"github.com/stretchr/testify/require"
)

var testSchema = `type User {
	Name: String
	Stuff: [String]
	Friend: User
}`

var testQuery = `mutation {
	createUser(input: {Name: "Bob", Stuff: ["one", "two"], Friend: {Name: "Alice"}}) {
		Name
		Stuff
		Friend {
			Name
		}
	}
}`

func TestBasicQuery(t *testing.T) {
	ctx := context.Background()

	db, err := New(ctx, testSchema, data.NewMemoryStore())
	require.NoError(t, err)

	res, err := db.Execute(ctx, graphql.QueryParams{
		Query: testQuery,
	})
	require.NoError(t, err)

	out, err := json.Marshal(res)
	require.NoError(t, err)

	fmt.Printf("%s\n", out)

	res, err = db.Execute(ctx, graphql.QueryParams{
		Query: `query {
			User {
				Name
				Stuff
			}
		}`,
	})
	require.NoError(t, err)

	out, err = json.Marshal(res)
	require.NoError(t, err)

	fmt.Printf("%s\n", out)
}
