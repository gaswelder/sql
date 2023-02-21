package sql

import (
	"errors"
	"fmt"
)

type binaryOperatorNode struct {
	op    string
	left  expression
	right expression
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

func (e binaryOperatorNode) String() string {
	return fmt.Sprintf("%s = %s", e.left.String(), e.right.String())
}

type fbinaryOr struct {
	left  expression
	right expression
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
