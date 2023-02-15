package sql

import (
	"fmt"
	"strings"
)

var aggregates = map[string]func(args []expression, rows []Row) (Value, error){
	"count": func(args []expression, rows []Row) (Value, error) {
		if len(args) != 1 {
			return Value{}, fmt.Errorf("unimplemented arguments variant for count: %s", args)
		}
		if _, ok := args[0].(*star); !ok {
			return Value{}, fmt.Errorf("unimplemented arguments variant for count: %s", args)
		}
		return Value{Int, len(rows)}, nil
	},
}

func isAggregate(name string) bool {
	_, ok := aggregates[strings.ToLower(name)]
	return ok
}

type aggregate struct {
	name string
	args []expression
}

func (e aggregate) eval(x Row, rows []Row) (Value, error) {
	f, ok := aggregates[strings.ToLower(e.name)]
	if !ok {
		return Value{}, fmt.Errorf("unknown aggregate: %s", e.name)
	}
	return f(e.args, rows)
}

func (e aggregate) String() string {
	return fmt.Sprintf("%s(%s)", e.name, "*")
}
