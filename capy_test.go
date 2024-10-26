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
	Friends: [User]
}`

var testQuery = `mutation {
	createUser(input: {Name: "Bob", Stuff: ["one", "two"], Friends: [{Name: "Alice"}]}) {
		_link
		Name
		Stuff
		Friends {
			Name
		}
	}
}`

func TestBasicQuery(t *testing.T) {
	ctx := context.Background()

	db, err := Open(ctx, testSchema, data.NewMemoryStore())
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
			User(link: "bafyrgqboaxlfczir6shprxuk5qlloir3a27b4tq6lctxr5ob4vbwhbnvylhc5gspsbiu5nsmvsbjl7utoawpjisua6mlxv3ibooaoedi746ks") {
				Name
			}
		}`,
	})
	require.NoError(t, err)

	out, err = json.Marshal(res)
	require.NoError(t, err)

	fmt.Printf("%s\n", out)
}
