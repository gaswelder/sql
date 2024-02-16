package sql

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFunctions(t *testing.T) {
	engine := New(map[string]Table{})
	cases := []struct {
		expr   string
		result any
	}{
		{`SUBSTRING( 'back yard',1 ,4 )`, Value{String, "back"}},
		{`SUBSTRING( 'back yard', -1 , -4 )`, Value{String, "yard"}},
		{`SUBSTRING( 'back yard', 6 )`, Value{String, "yard"}},
		{`CARDINALITY(ARRAY[1, 2, 3])`, Value{Int, 3}},
		{`array_contains(array[1,2,3], 2)`, Value{Bool, true}},
		{`CAST('1' as INT)`, Value{Int, 1}},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			r, err := engine.ExecString(`select ` + c.expr)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(r[0][0].Data, c.result); diff != "" {
				t.Fatalf(`%s`, diff)
			}
		})
	}
}
