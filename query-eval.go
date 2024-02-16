package sql

import (
	"errors"
	"fmt"
	"strings"
)

func (e aggregate) eval(x Row, rows []Row) (Value, error) {
	f, ok := aggregates[strings.ToLower(e.name)]
	if !ok {
		return Value{}, fmt.Errorf("unknown aggregate: %s", e.name)
	}
	return f(e.args, rows)
}

func (e binaryOperatorNode) eval(x Row, group []Row) (Value, error) {
	a, err := e.left.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	b, err := e.right.eval(x, group)
	if err != nil {
		return Value{}, err
	}
	var r bool
	switch e.op {
	case "=":
		r, err = a.eq(b)
	case ">":
		r, err = a.greaterThan(b)
	case "<":
		r, err = a.lessThan(b)
	default:
		return Value{}, fmt.Errorf("unsupported binary operator: %s", e.op)
	}
	return Value{Bool, r}, err
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

func (s *star) eval(x Row, g []Row) (Value, error) {
	return Value{}, fmt.Errorf("invalid use of star selector")
}

func (e columnRef) eval(x Row, group []Row) (Value, error) {
	for _, cell := range x {
		if e.Table != "" && !strings.EqualFold(e.Table, cell.TableName) {
			continue
		}
		if !strings.EqualFold(e.Column, cell.Name) {
			continue
		}
		return cell.Data, nil
	}
	return Value{}, fmt.Errorf("couldn't find %s in a row", e)
}

func (f functionkek) eval(r Row, group []Row) (Value, error) {
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
