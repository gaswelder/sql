package sql

// Query is a syntax tree that represents a query.
type Query struct {
	From      any
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

type tableName struct {
	Name string
}

type selector struct {
	Expr  expression
	Alias string
}

type as struct {
	Expr   expression
	TypeID ValueTypeID
}

// expression is a node in an SQL expression tree.
type expression interface {
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
	Name string
	Args []expression
}

// columnRef is an expression node that refers to a column.
type columnRef struct {
	Table  string
	Column string
}

type aggregate struct {
	Name string
	Args []expression
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
