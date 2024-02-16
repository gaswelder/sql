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
