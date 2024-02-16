package sql

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTrailing(t *testing.T) {
	_, err := Parse(`select app.id from app kek`)
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if diff := cmp.Diff("unexpected token: [identifier kek]", err.Error()); diff != "" {
		t.Fatalf("%s", diff)
	}
}

func TestParse2(t *testing.T) {
	cases := []struct {
		s string
		r any
	}{
		{
			`select id from app`,
			Query{
				From: fromspec{
					Kind: kindTableName,
					Tn:   &tableName{"app"},
				},
				Selectors: []selector{
					{Expr: &columnRef{Column: "id"}},
				},
			},
		},
		{
			`select count(*) from t`,
			Query{
				From: fromspec{
					Kind: kindTableName,
					Tn:   &tableName{"t"},
				},
				Selectors: []selector{
					{Expr: &aggregate{Name: "count", Args: []expression{&star{}}}},
				},
			},
		},
		{
			`select CAST('1' AS int)`,
			Query{
				From: fromspec{},
				Selectors: []selector{
					{Expr: &functionkek{
						Name: "CAST",
						Args: []expression{
							&as{&Value{String, "1"}, Int},
						},
					}},
				},
			},
		},
	}
	for _, c := range cases {
		q, err := Parse(c.s)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(q, c.r); diff != "" {
			t.Fatalf("%s", diff)
		}
	}

}
