package sql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
)

type jsonStream struct {
	_init    bool
	dec      *json.Decoder
	schema   map[string]ValueTypeID
	firstRow map[string]Value
}

func JsonStream(r io.Reader) *jsonStream {
	br := bufio.NewReader(r)
	dec := json.NewDecoder(br)
	return &jsonStream{
		dec: dec,
	}
}

func (s *jsonStream) init() error {
	if s._init {
		return nil
	}
	s._init = true

	// Read one row as map.
	var m map[string]any
	err := s.dec.Decode(&m)

	// If the data source is empty, treat it as an empty table.
	if err == io.EOF {
		s.schema = map[string]ValueTypeID{}
		return nil
	}

	// Infer types from the first row.
	schema := map[string]ValueTypeID{}
	for k, v := range m {
		schema[k] = guessType(v)
	}
	s.schema = schema

	// Stash the first row for reuse.
	s.firstRow = s.parse(m)
	return nil
}

func (s *jsonStream) ColumnNames() []string {
	if err := s.init(); err != nil {
		panic(err)
	}
	var r []string
	for k := range s.schema {
		r = append(r, k)
	}
	return r
}

func (s *jsonStream) read() (map[string]Value, error) {
	// read one line, parse as map
	var m map[string]any
	err := s.dec.Decode(&m)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.parse(m), nil
}

func (s *jsonStream) parse(m map[string]any) map[string]Value {
	row := map[string]Value{}
	for k, t := range s.schema {
		row[k] = Value{t, m[k]}
	}
	return row
}

func (s *jsonStream) GetRows() func() (map[string]Value, error) {
	first := true
	return func() (map[string]Value, error) {
		if err := s.init(); err != nil {
			return nil, err
		}
		if first {
			first = false
			return s.firstRow, nil
		}
		return s.read()
	}
}

func castToInt(item map[string]any) {
	for k, v := range item {
		if f, ok := v.(float64); ok && float64(int(f)) == f {
			item[k] = int(f)
		}
	}
}

func guessType(x any) ValueTypeID {
	switch x.(type) {
	case string:
		return String
	case float64:
		return Double
	case int:
		return Int
	case []interface{}:
		return Array
	default:
		panic(fmt.Errorf("unexpected value type: %s", reflect.TypeOf(x)))
	}
}

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

func JsonTable(path string) dummy {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var items []map[string]any
	err = json.Unmarshal(data, &items)
	if err != nil {
		panic(err)
	}
	for _, item := range items {
		castToInt(item)
	}

	extend := func(t1, t2 ValueTypeID) ValueTypeID {
		if t1 == undefined {
			return t2
		}
		if t1 == t2 {
			return t1
		}
		if t1 == Int && t2 == String {
			return String
		}
		if t1 == String && t2 == Int {
			return String
		}
		panic(fmt.Errorf("extend %s %s", getValueTypeName(t1), getValueTypeName(t2)))
	}

	schema := map[string]ValueTypeID{}
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
