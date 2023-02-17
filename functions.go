package main

import (
	"fmt"
	"strings"
)

// Alphabetical list of functions http://dev.cs.ovgu.de/db/sybase9/help/dbrfen9/00000123.htm

func function(name string, args []Value) (Value, error) {
	switch strings.ToLower(name) {
	// array_contains(array, item)
	case "array_contains":
		if len(args) != 2 {
			return Value{}, fmt.Errorf("the %s function expects 2 arguments", strings.ToUpper(name))
		}
		array := args[0]
		item := args[1]
		for _, x := range array.Data.([]Value) {
			e, err := x.eq(item)
			if err != nil {
				return Value{Bool, false}, err
			}
			if e {
				return Value{Bool, true}, nil
			}
		}
		return Value{Bool, true}, nil

	// cardinality(array)
	case "cardinality":
		if len(args) != 1 {
			return Value{}, fmt.Errorf("the %s function expects 1 argument", strings.ToUpper(name))
		}
		array := args[0]
		return Value{Int, len(array.Data.([]Value))}, nil

	// substring(string, start)
	// substring(string, start, b)
	case "substring":
		if len(args) != 2 && len(args) != 3 {
			return Value{}, fmt.Errorf("the %s function expects 2 or 3 arguments", strings.ToUpper(name))
		}
		value := []rune(args[0].Data.(string))
		norm := func(x int) (int, error) {
			switch true {
			case x > 0:
				return x - 1, nil
			case x < 0:
				return x + len(value), nil
			default:
				return x, fmt.Errorf("the %s function's start and length arguments are 1-based, not 0-based", strings.ToUpper(name))
			}
		}

		switch len(args) {
		case 2:
			start, err := norm(args[1].Data.(int))
			if err != nil {
				return Value{}, err
			}
			return Value{String, string(value[start:])}, nil
		case 3:
			start, err := norm(args[1].Data.(int))
			if err != nil {
				return Value{}, err
			}
			length, err := norm(args[2].Data.(int))
			if err != nil {
				return Value{}, err
			}
			if length < start {
				length, start = start, length
			}
			return Value{String, string(value[start : length+1])}, nil
		default:
			panic("broken switch")
		}
	default:
		return Value{}, fmt.Errorf("unknown function %s", name)
	}
}
