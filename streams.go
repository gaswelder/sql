package sql

type stream[T any] struct {
	next func() (T, bool, error)
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

func toStream1(f func() (Row, error)) *stream[Row] {
	return &stream[Row]{
		func() (Row, bool, error) {
			r, err := f()
			if err != nil {
				return nil, false, err
			}
			if r == nil {
				return nil, true, nil
			}
			return r, false, nil
		},
	}
}

func toStream2(f func() ([]Row, error)) *stream[[]Row] {
	return &stream[[]Row]{
		func() ([]Row, bool, error) {
			r, err := f()
			if err != nil {
				return nil, false, err
			}
			if r == nil {
				return nil, true, nil
			}
			return r, false, nil
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

func arrayIterator(xs []Row) func() (Row, error) {
	i := 0
	return func() (Row, error) {
		if i >= len(xs) {
			return nil, nil
		}
		r := xs[i]
		i++
		return r, nil
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

func tableIter(tableName string, s func() (map[string]Value, error)) func() (Row, error) {
	return func() (Row, error) {
		row, err := s()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		var result []Cell
		for name, value := range row {
			result = append(result, Cell{tableName, name, value})
		}
		return result, nil
	}
}

func rewindable(xs func() (Row, error)) (func() (Row, error), func()) {
	var items []Row
	i := 0
	cached := false
	rewind := func() {
		cached = true
		i = 0
	}
	next := func() (Row, error) {
		if cached {
			if i >= len(items) {
				return nil, nil
			}
			x := items[i]
			i++
			return x, nil
		} else {
			x, err := xs()
			if err != nil {
				return nil, err
			}
			if x == nil {
				return nil, nil
			}
			items = append(items, x)
			return x, nil
		}
	}
	return next, rewind
}
