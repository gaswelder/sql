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

func (t tableName) String() string {
	return t.Name
}

func (s fromspec) String() string {
	switch s.kind {
	case kindNil:
		return ""
	case kindTableName:
		return s.tn.String()
	case kindSubquery:
		return "(subquery)"
	default:
		return "unknown kind"
	}
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
