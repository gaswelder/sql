package sql

import (
	"fmt"
	"strings"
)

type Stream[T any] struct {
	name string
	gen  func() (T, bool, error)
}

func (s *Stream[T]) Next() (T, bool, error) {
	t, done, err := s.gen()
	// fmt.Println("next", s.name, t, done, err)
	return t, done, err
}

func (r Row) String() string {
	b := strings.Builder{}
	b.WriteString("Row {")
	for i, c := range r {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%s.%s=%s:%s", c.TableName, c.Name, getValueTypeName(c.Data.Type), c.Data))
	}
	b.WriteString("}")
	return b.String()
}

func (s *Stream[T]) filter(f func(T) (bool, error)) *Stream[T] {
	var t T
	return &Stream[T]{
		s.name + ".filter",
		func() (T, bool, error) {
			for {
				r, done, err := s.Next()
				if done || err != nil {
					return t, done, err
				}
				ok, err := f(r)
				if err != nil {
					return r, false, err
				}
				if ok {
					return r, false, nil
				}
			}
		},
	}
}

func (s *Stream[T]) limit(take int) *Stream[T] {
	i := 0
	var t T
	return &Stream[T]{
		s.name + ".limit",
		func() (T, bool, error) {
			if i >= take {
				return t, true, nil
			}
			i++
			return s.Next()
		}}
}

func (s *Stream[T]) Consume() ([]T, error) {
	var groups []T
	for {
		r, done, err := s.Next()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		groups = append(groups, r)
	}
	return groups, nil
}

func mapStream[T, U any](s *Stream[T], f func(T) (U, error)) *Stream[U] {
	var u U
	return &Stream[U]{
		s.name + ".conv",
		func() (U, bool, error) {
			rows, done, err := s.Next()
			if err != nil || done {
				return u, done, err
			}
			val, err := f(rows)
			return val, false, err
		},
	}
}

func rewindable(xs *Stream[Row]) (*Stream[Row], func() *Stream[Row]) {
	var items []Row
	rewind := func() *Stream[Row] {
		return arrstream(items)
	}
	s := &Stream[Row]{
		xs.name + ".rewindable",
		func() (Row, bool, error) {
			x, done, err := xs.Next()
			if err != nil || done {
				return nil, done, err
			}
			items = append(items, x)
			return x, false, nil
		},
	}
	return s, rewind
}

func tablestream(tableName string, s func() (map[string]Value, error)) *Stream[Row] {
	return &Stream[Row]{
		"table " + tableName,
		func() (Row, bool, error) {
			row, err := s()
			if err != nil {
				return nil, false, err
			}
			if row == nil {
				return nil, true, nil
			}
			var result []Cell
			for name, value := range row {
				result = append(result, Cell{tableName, name, value})
			}
			return result, false, nil
		},
	}
}

func arrstream[T any](xs []T) *Stream[T] {
	var t T
	i := 0
	return &Stream[T]{
		"array",
		func() (T, bool, error) {
			if i >= len(xs) {
				return t, true, nil
			}
			r := xs[i]
			i++
			return r, false, nil
		},
	}
}
