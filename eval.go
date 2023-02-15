package sql

import (
	"errors"
	"fmt"
	"strings"
)

func (e aggregate) eval(rows []Row) Value {
	switch e.name {
	case "count(*)":
		return Value{Int, len(rows)}
	default:
		panic(fmt.Errorf("unknown aggregate: %s", e.name))
	}
}
func (e aggregate) String() string {
	return e.name
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

func (e columnRef) eval(x Row, group []Row) (Value, error) {
	return x.get(e.Table, e.Column), nil
}
func (e columnRef) String() string {
	if e.Table == "" {
		return fmt.Sprintf("\"%s\"", e.Column)
	}
	return fmt.Sprintf("\"%s\".\"%s\"", e.Table, e.Column)
}

func (e fbinaryOr) eval(x Row, group []Row) (Value, error) {
	a, err := e.left.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	if a.Type != Bool {
		return Value{}, errors.New("left-hand side does not evaluate to bool: " + e.left.String())
	}
	if a.Data.(bool) {
		return a, nil
	}

	b, err := e.right.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	if b.Type != Bool {
		return Value{}, errors.New("right-hand side does not evaluate to bool: " + e.right.String())
	}
	return b, nil
}

func (e fbinaryOr) String() string {
	return fmt.Sprintf("%s OR %s", e.left.String(), e.right.String())
}

func (e feq) eval(x Row, group []Row) (Value, error) {
	a, err := e.left.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	b, err := e.right.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	return Value{Bool, a.Data == b.Data}, nil
}

func (e feq) String() string {
	return fmt.Sprintf("%s = %s", e.left.String(), e.right.String())
}

func (c Value) eval(x Row, group []Row) (Value, error) {
	return c, nil
}

func (e Value) String() string {
	return fmt.Sprintf("%v", e.Data)
}
