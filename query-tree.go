package sql

// Query is a syntax tree that represents a query.
type Query struct {
	From      fromspec
	Joins     []joinspec
	Filter    expression
	Selectors []selector
	GroupBy   []expression
	OrderBy   []orderspec
	Limit     struct {
		Set   bool
		Value int
	}
}

const (
	kindNil = iota
	kindTableName
	kindSubquery
)

type fromspec struct {
	kind int
	q    *Query
	tn   *tableName
}

type tableName struct {
	Name string
}

type selector struct {
	expr  expression
	alias string
}

// expression is a node in an SQL expression tree.
type expression interface {
	eval(x Row, g []Row) (Value, error)
	String() string
}

type joinspec struct {
	Table     *tableName
	Condition expression
}

type orderspec struct {
	desc bool
	expr expression
}

type functionkek struct {
	name string
	args []expression
}

// columnRef is an expression node that refers to a column.
type columnRef struct {
	Table  string
	Column string
}

type aggregate struct {
	name string
	args []expression
}

type binaryOperatorNode struct {
	op    string
	left  expression
	right expression
}

type fbinaryOr struct {
	left  expression
	right expression
}

type star struct {
	//
}
