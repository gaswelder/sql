package sql

import (
	"fmt"
	"reflect"
)

// traverse calls f on all nodes inside x, where x is a Query or any node
// inside of it.
func traverse(x any, f func(any) error) error {
	switch v := x.(type) {
	case *Value, *columnRef:
		return f(v)
	case *functionkek:
		if err := f(v); err != nil {
			return err
		}
		for _, arg := range v.Args {
			if err := traverse(arg, f); err != nil {
				return err
			}
		}
		return nil
	case *fbinaryOr:
		if err := f(v); err != nil {
			return err
		}
		if err := traverse(v.left, f); err != nil {
			return err
		}
		if err := traverse(v.right, f); err != nil {
			return err
		}
		return nil
	case *binaryOperatorNode:
		if err := f(v); err != nil {
			return err
		}
		if err := traverse(v.left, f); err != nil {
			return err
		}
		if err := traverse(v.right, f); err != nil {
			return err
		}
		return nil
	case *Query:
		if err := f(v.From); err != nil {
			return err
		}
		for _, sel := range v.Selectors {
			e1, ok := sel.Expr.(expression)
			if !ok {
				continue
			}
			if err := traverse(e1, f); err != nil {
				return err
			}
		}
		for _, j := range v.Joins {
			if err := f(j.Table); err != nil {
				return err
			}
			if err := traverse(j.Condition, f); err != nil {
				return err
			}
		}
		if v.Filter != nil {
			if err := traverse(v.Filter, f); err != nil {
				return err
			}
		}
		for _, g := range v.GroupBy {
			if err := traverse(g, f); err != nil {
				return err
			}
		}
		for _, o := range v.OrderBy {
			if err := traverse(o.expr, f); err != nil {
				return err
			}
		}
		return nil
	case *star:
		return nil
	case *aggregate:
		// if err := f(v); err != nil {
		// 	return err
		// }
		for _, arg := range v.Args {
			if err := traverse(arg, f); err != nil {
				return err
			}
		}
		return nil
	default:
		panic(fmt.Errorf("don't know how to traverse %s", reflect.TypeOf(x)))
	}
}
