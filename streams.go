package main

type stream[T any] struct {
	next func() (T, bool, error)
}

func (s *stream[T]) filter(f func(T) (bool, error)) *stream[T] {
	var t T
	return &stream[T]{
		func() (T, bool, error) {
			for {
				r, done, err := s.next()
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

func (s *stream[T]) limit(take int) *stream[T] {
	i := 0
	var t T
	return &stream[T]{
		func() (T, bool, error) {
			if i >= take {
				return t, true, nil
			}
			i++
			return s.next()
		}}
}

func (s *stream[T]) consume() ([]T, error) {
	var groups []T
	for {
		r, done, err := s.next()
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

func conv[T, U any](s *stream[T], f func(T) (U, error)) *stream[U] {
	var u U
	return &stream[U]{
		func() (U, bool, error) {
			rows, done, err := s.next()
			if err != nil || done {
				return u, done, err
			}
			val, err := f(rows)
			return val, false, err
		},
	}
}

func toRowsStream(s *stream[Row]) *RowsStream {
	return &RowsStream{
		func() (Row, error) {
			r, done, err := s.next()
			if err != nil {
				return nil, err
			}
			if done {
				return nil, nil
			}
			return r, nil
		},
	}
}

func consume(it func() (Row, error)) ([]Row, error) {
	var filtered []Row
	for {
		r, err := it()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		filtered = append(filtered, r)
	}
	return filtered, nil
}

func rewindable(xs *stream[Row]) (*stream[Row], func() *stream[Row]) {
	var items []Row
	rewind := func() *stream[Row] {
		return arrstream(items)
	}
	s := &stream[Row]{
		func() (Row, bool, error) {
			x, done, err := xs.next()
			if err != nil || done {
				return nil, done, err
			}
			items = append(items, x)
			return x, false, nil
		},
	}
	return s, rewind
}

func tablestream(tableName string, s func() (map[string]Value, error)) *stream[Row] {
	return &stream[Row]{
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

func arrstream[T any](xs []T) *stream[T] {
	var t T
	i := 0
	return &stream[T]{
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
