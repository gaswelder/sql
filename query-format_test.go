package sql

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFormatter(t *testing.T) {
	type tcase struct {
		input, want string
	}
	cc := []tcase{
		{
			input: `select app.id, count(*), substring(app.id, 0, 1)
	from user
	join app on substring(user.name, 0, 1) = substring(app.namespace, 0, 1)
	group by user.id, substring(app.id, 0, 1)
	`,
			want: `SELECT "app"."id", count(*), substring("app"."id", 0, 1) FROM "user" JOIN "app" ON substring("user"."name", 0, 1) = substring("app"."namespace", 0, 1) GROUP BY "user"."id", substring("app"."id", 0, 1)`,
		},
		{
			"select app.id from app",
			`SELECT "app"."id" FROM "app"`,
		},
		{
			`select id from app`,
			`SELECT "id" FROM "app"`,
		},
	}
	for _, c := range cc {
		q, err := Parse(c.input)
		if err != nil {
			t.Error(err)
			continue
		}
		got := format(q)
		diff := cmp.Diff(c.want, got)
		if diff != "" {
			t.Errorf("\nwanted:\n%s\ngot:\n%s\ndiff:\n%s\n", c.want, got, diff)
		}
	}
}
