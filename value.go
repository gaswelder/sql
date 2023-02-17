package main

import "fmt"

type ValueType int

const (
	undefined ValueType = iota
	String    ValueType = 1 + iota
	Int
	Double
	Bool
	Array
	JSON
)

type Value struct {
	Type ValueType
	Data any
}

func tn(t ValueType) string {
	switch t {
	case String:
		return "String"
	case Int:
		return "Int"
	case Double:
		return "Double"
	case Bool:
		return "Bool"
	case Array:
		return "Array"
	case JSON:
		return "JSON"
	default:
		panic(fmt.Errorf("unexpected value type: %d", t))
	}
}

func (c Value) eval(x Row, group []Row) (Value, error) {
	return c, nil
}

func (e Value) String() string {
	return fmt.Sprintf("%v", e.Data)
}

func eq(a, b Value) (bool, error) {
	if a.Type != b.Type {
		return false, fmt.Errorf("can't compare values of different types: %s and %s", tn(a.Type), tn(b.Type))
	}
	switch a.Type {
	case String, Int:
		return a.Data == b.Data, nil
	default:
		return false, fmt.Errorf("eq: don't know how to compare values of type %s", tn(a.Type))
	}
}

func (a Value) lessThan(b Value) (bool, error) {
	if a.Type != b.Type {
		return false, fmt.Errorf("can't compare values of different types: %s and %s", tn(a.Type), tn(b.Type))
	}
	switch a.Type {
	case Int:
		return a.Data.(int) < b.Data.(int), nil
	case Double:
		return a.Data.(float64) < b.Data.(float64), nil
	default:
		return false, fmt.Errorf("lessThan: don't know how to compare values of type %s", tn(a.Type))
	}
}
