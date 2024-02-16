package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func eval(node any, row Row, group []Row) (Value, error) {
	switch e := node.(type) {
	case *Value:
		return *e, nil

	case *columnRef:
		return evalColumnRef(e, row, group)

	case *aggregate:
		switch strings.ToLower(e.Name) {
		case "count":
			return evalCount(e.Args, group)
		case "min":
			return evalMin(e.Args, group)
		}
		return Value{}, fmt.Errorf("unknown aggregate: %s", e.Name)

	case *functionkek:
		return evalFunction(e, row, group)

	case *binaryOperatorNode:
		return evalBinaryOp(e, row, group)

	case *fbinaryOr:
		return evalBinaryOr(e, row, group)

	default:
		panic(fmt.Sprintf("unknown node in eval: %v", reflect.TypeOf(node)))
	}
}

func evalBinaryOp(v *binaryOperatorNode, x Row, rows []Row) (Value, error) {
	a, err := eval(v.left, x, rows)
	if err != nil {
		return Value{}, err
	}
	b, err := eval(v.right, x, rows)
	if err != nil {
		return Value{}, err
	}
	var r bool
	switch v.op {
	case "=":
		r, err = a.eq(b)
	case ">":
		r, err = a.greaterThan(b)
	case "<":
		r, err = a.lessThan(b)
	default:
		return Value{}, fmt.Errorf("unsupported binary operator: %s", v.op)
	}
	return Value{Bool, r}, err
}

func evalBinaryOr(e *fbinaryOr, x Row, group []Row) (Value, error) {
	a, err := eval(e.left, x, group)
	if err != nil {
		return Value{}, err
	}
	if a.Type != Bool {
		return Value{}, errors.New("left-hand side does not evaluate to bool: " + e.left.String())
	}
	if a.Data.(bool) {
		return a, nil
	}
	b, err := eval(e.right, x, group)
	if err != nil {
		return Value{}, err
	}
	if b.Type != Bool {
		return Value{}, errors.New("right-hand side does not evaluate to bool: " + e.right.String())
	}
	return b, nil
}

func evalColumnRef(e *columnRef, x Row, group []Row) (Value, error) {
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

func evalFunction(f *functionkek, r Row, group []Row) (Value, error) {
	args := make([]Value, len(f.Args))
	for i, argExpression := range f.Args {
		exprResult, err := eval(argExpression, r, group)
		if err != nil {
			return exprResult, err
		}
		args[i] = exprResult
	}
	return function(f.Name, args)
}

func evalCount(args []expression, rows []Row) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("unimplemented arguments variant for count: %s", args)
	}
	if _, ok := args[0].(*star); !ok {
		return Value{}, fmt.Errorf("unimplemented arguments variant for count: %s", args)
	}
	return Value{Int, len(rows)}, nil
}

func evalMin(args []expression, rows []Row) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("unimplemented arguments variant for min: %s", args)
	}
	min := Value{Int, nil}
	for i, row := range rows {
		v, err := eval(args[0], row, rows)
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

func isAggregate(name string) bool {
	n := strings.ToLower(name)
	return n == "count" || n == "min"
}
