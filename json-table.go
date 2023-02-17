package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
)

type dummy []map[string]Value

func (data dummy) GetRows() func() (map[string]Value, error) {
	i := 0
	return func() (map[string]Value, error) {
		if i >= len(data) {
			return nil, nil
		}
		item := data[i]
		i++
		return item, nil
	}
}

func (data dummy) ColumnNames() []string {
	var s []string
	for k := range data[0] {
		s = append(s, k)
	}
	return s
}

func jsonTable(path string) dummy {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var items []map[string]any
	err = json.Unmarshal(data, &items)
	if err != nil {
		panic(err)
	}
	for i, item := range items {
		for k, v := range item {
			if f, ok := v.(float64); ok && float64(int(f)) == f {
				items[i][k] = int(f)
			}
		}
	}

	guessType := func(x any) ValueType {
		switch x.(type) {
		case string:
			return String
		case float64:
			return Double
		case int:
			return Int
		default:
			panic(fmt.Errorf("unexpected value type: %s", reflect.TypeOf(x)))
		}
	}

	extend := func(t1, t2 ValueType) ValueType {
		if t1 == undefined {
			return t2
		}
		if t1 == t2 {
			return t1
		}
		panic(fmt.Errorf("extend %s %s", tn(t1), tn(t2)))
	}

	schema := map[string]ValueType{}
	for _, item := range items {
		for k, v := range item {
			schema[k] = extend(schema[k], guessType(v))
		}
	}

	d := dummy{}
	for _, item := range items {
		row := map[string]Value{}
		for k, t := range schema {
			row[k] = Value{t, item[k]}
		}
		d = append(d, row)
	}
	return d
}
