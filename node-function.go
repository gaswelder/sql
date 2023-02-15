package sql

import "strings"

type functionkek struct {
	name string
	args []expression
}

func (f functionkek) String() string {
	b := strings.Builder{}
	b.WriteString(f.name)
	b.WriteString("(")
	for i, a := range f.args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	b.WriteString(")")
	return b.String()
}

func (f functionkek) eval(r Row, group []Row) (Value, error) {
	if f.name == "min" {
		min := Value{Int, nil}
		for i, row := range group {
			v, err := f.args[0].eval(row, group)
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
	}
	args := make([]Value, len(f.args))
	for i, argExpression := range f.args {
		exprResult, err := argExpression.eval(r, group)
		if err != nil {
			return exprResult, err
		}
		args[i] = exprResult
	}

	return function(f.name, args)
}
