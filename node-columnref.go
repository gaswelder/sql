package sql

import "fmt"

// columnRef is an expression node that refers to a column.
type columnRef struct {
	Table  string
	Column string
}

func (e columnRef) String() string {
	if e.Table == "" {
		return fmt.Sprintf("\"%s\"", e.Column)
	}
	return fmt.Sprintf("\"%s\".\"%s\"", e.Table, e.Column)
}

func (e columnRef) eval(x Row, group []Row) (Value, error) {
	return x.get(e.Table, e.Column), nil
}
