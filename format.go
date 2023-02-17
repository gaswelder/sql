package main

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
		r.WriteString(fmt.Sprintf("%v", s.expr))
		if s.alias != "" {
			r.WriteString(fmt.Sprintf(" AS %s", s.alias))
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

func formatRows(rr []Row) string {
	sb := strings.Builder{}
	for _, r := range rr {
		for i, c := range r {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s=%v", c.Name, c.Data))
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}
