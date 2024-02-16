package sql

import (
	"fmt"
	"strings"
)

type ValueTypeID int

const (
	undefined ValueTypeID = iota
	String    ValueTypeID = 1 + iota
	Int
	Double
	Bool
	Array
	JSON
)

type Value struct {
	Type ValueTypeID
	Data any
}

func getTypeID(s string) ValueTypeID {
	switch strings.ToLower(s) {
	case "int":
		return Int
	}
	return undefined
}

func getValueTypeName(t ValueTypeID) string {
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

func (e Value) String() string {
	return fmt.Sprintf("%v", e.Data)
}

func (a Value) eq(b Value) (bool, error) {
	if a.Type != b.Type {
		return false, fmt.Errorf("can't compare values of different types: %s and %s", getValueTypeName(a.Type), getValueTypeName(b.Type))
	}
	switch a.Type {
	case String, Int:
		return a.Data == b.Data, nil
	default:
		return false, fmt.Errorf("eq: don't know how to compare values of type %s", getValueTypeName(a.Type))
	}
}

func (a Value) lessThan(b Value) (bool, error) {
	if a.Type != b.Type {
		return false, fmt.Errorf("can't compare values of different types: %s and %s", getValueTypeName(a.Type), getValueTypeName(b.Type))
	}
	if a.Data == nil || b.Data == nil {
		return false, nil
	}
	switch a.Type {
	case Int:
		return a.Data.(int) < b.Data.(int), nil
	case Double:
		return a.Data.(float64) < b.Data.(float64), nil
	default:
		return false, fmt.Errorf("lessThan: don't know how to compare values of type %s", getValueTypeName(a.Type))
	}
}

func (a Value) greaterThan(b Value) (bool, error) {
	eq, err := a.eq(b)
	if err != nil || eq {
		return false, err
	}
	lt, err := a.lessThan(b)
	return !lt, err
}
