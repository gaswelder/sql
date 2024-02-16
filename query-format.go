package sql

import (
	"fmt"
	"reflect"
	"strings"
)

func FormatQuery(q Query) string {
	r := strings.Builder{}

	r.WriteString(fmt.Sprintf("%8s", "SELECT"))
	for i, s := range q.Selectors {
		if i > 0 {
			r.WriteString(",")
		}
		r.WriteString(" ")
		r.WriteString(fmt.Sprintf("%v", s.Expr))
		if s.Alias != "" {
			r.WriteString(fmt.Sprintf(" AS %s", s.Alias))
		}
	}

	r.WriteString(fmt.Sprintf("\n%8s \"%s\"", "FROM", q.From))

	for i, j := range q.Joins {
		if i > 0 {
			r.WriteString(",")
		}
		r.WriteString(fmt.Sprintf("\n%8s \"%s\"", "JOIN", j.Table))
		r.WriteString(" ON ")
		r.WriteString(fmt.Sprintf("%v", j.Condition))
	}

	if q.Filter != nil {
		r.WriteString(fmt.Sprintf("\n%8s %s", "WHERE", q.Filter.String()))
	}

	if q.GroupBy != nil {
		r.WriteString(fmt.Sprintf("\n%8s ", "GROUP BY"))
		r.WriteString(fmt.Sprintf("%v", q.GroupBy))
	}
	if len(q.OrderBy) > 0 {
		r.WriteString(fmt.Sprintf("\n%8s", "ORDER BY"))
		for i, o := range q.OrderBy {
			if i > 0 {
				r.WriteString(",")
			}
			r.WriteString(" ")
			switch v := o.expr.(type) {
			case *columnRef:
				r.WriteString(v.String())
			case aggregate:
				r.WriteString(v.String())
			default:
				panic(fmt.Errorf("unexpected expression type in order by: %s", reflect.TypeOf(o.expr)))
			}
		}
	}
	if q.Limit.Set {
		r.WriteString(fmt.Sprintf("\n%8s %d", "LIMIT", q.Limit.Value))
	}
	return r.String()
}

func (s star) String() string {
	return "*"
}

func (e aggregate) String() string {
	sb := strings.Builder{}
	sb.WriteString(e.Name)
	sb.WriteString("(")
	for _, a := range e.Args {
		sb.WriteString(a.String())
	}
	sb.WriteString(")")
	return sb.String()
}

func (f functionkek) String() string {
	b := strings.Builder{}
	b.WriteString(f.name)
	b.WriteString("(")
	for i, a := range f.args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	b.WriteString(")")
	return b.String()
}

func (e fbinaryOr) String() string {
	return fmt.Sprintf("%s OR %s", e.left.String(), e.right.String())
}

func (e binaryOperatorNode) String() string {
	return fmt.Sprintf("%s = %s", e.left.String(), e.right.String())
}

func (e columnRef) String() string {
	if e.Table == "" {
		return fmt.Sprintf("\"%s\"", e.Column)
	}
	return fmt.Sprintf("\"%s\".\"%s\"", e.Table, e.Column)
}

func (t tableName) String() string {
	return t.Name
}

func (s fromspec) String() string {
	switch s.Kind {
	case kindNil:
		return ""
	case kindTableName:
		return s.Tn.String()
	case kindSubquery:
		return "(subquery)"
	default:
		return "unknown kind"
	}
}
