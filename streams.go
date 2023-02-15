package sql

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
