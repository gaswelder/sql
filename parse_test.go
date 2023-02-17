package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParser(t *testing.T) {
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

func format(q Query) string {
	r := strings.Builder{}

	r.WriteString("SELECT")
	for i, s := range q.Selectors {
		if i > 0 {
			r.WriteString(",")
		}
		r.WriteString(" ")
		r.WriteString(fmt.Sprintf("%v", s.expr))
		if s.alias != "" {
			r.WriteString(fmt.Sprintf(" AS %s", s.alias))
		}
	}

	r.WriteString(fmt.Sprintf(" %s \"%s\"", "FROM", q.From))

	for i, j := range q.Joins {
		if i > 0 {
			r.WriteString(",")
		}
		r.WriteString(fmt.Sprintf(" %s \"%s\"", "JOIN", j.Table))
		r.WriteString(" ON ")
		r.WriteString(fmt.Sprintf("%v", j.Condition))
	}

	if q.Filter != nil {
		r.WriteString(fmt.Sprintf(" %s %s", "WHERE", q.Filter.String()))
	}

	if len(q.GroupBy) > 0 {
		r.WriteString(fmt.Sprintf(" %s ", "GROUP BY"))
		for i, g := range q.GroupBy {
			if i > 0 {
				r.WriteString(", ")
			}
			r.WriteString(g.String())
		}
	}
	return r.String()
}

func TestTrailing(t *testing.T) {
	_, err := Parse(`select app.id from app kek`)
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if diff := cmp.Diff("unexpected token: [identifier kek]", err.Error()); diff != "" {
		t.Fatalf("%s", diff)
	}
}
