package sql

// columnRef is an expression node that refers to a column.
type columnRef struct {
	Table  string
	Column string
}
