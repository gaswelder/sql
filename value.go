package sql

import (
	"fmt"
	"strconv"
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

func getTypeName(t ValueTypeID) string {
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
		return false, fmt.Errorf("can't compare values of different types: %s and %s", getTypeName(a.Type), getTypeName(b.Type))
	}
	switch a.Type {
	case String, Int:
		return a.Data == b.Data, nil
	default:
		return false, fmt.Errorf("eq: don't know how to compare values of type %s", getTypeName(a.Type))
	}
}

func (a Value) lessThan(b Value) (bool, error) {
	if a.Type != b.Type {
		return false, fmt.Errorf("can't compare values of different types: %s and %s", getTypeName(a.Type), getTypeName(b.Type))
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
		return false, fmt.Errorf("lessThan: don't know how to compare values of type %s", getTypeName(a.Type))
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

func (a Value) cast(typeID ValueTypeID) (Value, error) {
	if typeID == a.Type {
		return a, nil
	}
	switch a.Type {
	case String:
		switch typeID {
		case Int:
			i, err := strconv.Atoi(a.Data.(string))
			if err != nil {
				return Value{}, err
			}
			return Value{Int, i}, nil
		}
	}
	return Value{}, fmt.Errorf("conversion from %s to %s not implemented", getTypeName(a.Type), getTypeName(typeID))
}
