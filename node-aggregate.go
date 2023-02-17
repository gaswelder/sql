package main

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
	"min": func(args []expression, rows []Row) (Value, error) {
		if len(args) != 1 {
			return Value{}, fmt.Errorf("unimplemented arguments variant for min: %s", args)
		}
		min := Value{Int, nil}
		for i, row := range rows {
			v, err := args[0].eval(row, rows)
			if err != nil {
				return Value{}, err
			}
			if i == 0 {
				min = v
				continue
			}
			less, err := v.lessThan(min)
			if err != nil {
				return Value{}, err
			}
			if less {
				min = v
			}
		}
		return min, nil
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
	sb := strings.Builder{}
	sb.WriteString(e.name)
	sb.WriteString("(")
	for _, a := range e.args {
		sb.WriteString(a.String())
	}
	sb.WriteString(")")
	return sb.String()
}
