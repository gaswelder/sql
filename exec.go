package sql

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Row is a collection of cells.
type Row []Cell

// Cell is single piece of data, always part of a row.
type Cell struct {
	// Name of the table or the subquery this cell came from.
	TableName string
	// Name of the column. Should be unique for a row.
	Name string
	// Value.
	Data Value
}

// Engine parses and executes SQL queries.
type Engine struct {
	tables map[string]Table
}

type Table interface {
	GetRows() func() (map[string]Value, error)
	ColumnNames() []string
}

// New returns a new instance of the SQL engine.
func New(tables map[string]Table) Engine {
	return Engine{tables}
}

// ExecString parses and executes a string SQL query agains the backend.
func (e Engine) ExecString(sql string) ([]Row, error) {
	q, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	s, err := e.Exec(q)
	if err != nil {
		return nil, err
	}
	return s.Consume()
}

func findTable(e Engine, name string) (Table, error) {
	options := []string{}
	for k := range e.tables {
		if strings.EqualFold(name, k) {
			options = append(options, k)
		}
	}
	if len(options) == 0 {
		return nil, fmt.Errorf("table not found: %s", name)
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("ambiguous table name: %s (%s)", name, strings.Join(options, ", "))
	}
	return e.tables[options[0]], nil
}

// Exec runs the query and returns the results.
func (e Engine) Exec(Q Query) (*Stream[Row], error) {

	// 	## SQL's logical order of operations
	// 1. from, join, apply
	// 2. where
	// 3. connect by (Oracle)
	// 4. group by
	// 5. aggregations
	// 6. having
	// 7. window
	// 8. model (Oracle)
	// 9. select
	// 10. distinct
	// 11. union, intersect, except
	// 12. order by
	// 13. offset
	// 14. limit
	// 15. for update

	if len(Q.Selectors) == 0 {
		return nil, fmt.Errorf("empty selectors list")
	}
	// Define the base input
	var input *Stream[Row]
	switch v := Q.From.(type) {
	case nil:
		// Empty FROM
		input = arrstream([]Row{{}})
	case *tableName:
		table, err := findTable(e, v.Name)
		if err != nil {
			return nil, err
		}
		input = tablestream(v.Name, table.GetRows())
	case *Query:
		var err error
		input, err = e.Exec(*v)
		if err != nil {
			return nil, err
		}
	default:
		panic(fmt.Errorf("unhandled from type: %v", reflect.TypeOf(Q.From)))
	}

	// Join other inputs
	for _, j := range Q.Joins {
		table, ok := e.tables[j.Table.Name]
		if !ok {
			return nil, fmt.Errorf("table not found: %s", j.Table.Name)
		}
		more := tablestream(j.Table.Name, table.GetRows())
		input = joinTables(input, more).filter(func(r Row) (bool, error) {
			ev, err := eval(j.Condition, r, nil)
			if err != nil {
				return false, err
			}
			return ev.Data.(bool), nil
		})
	}

	if Q.Filter != nil {
		input = input.filter(func(r Row) (bool, error) {
			ok, err := eval(Q.Filter, r, nil)
			if err != nil {
				return false, fmt.Errorf("failed to calculate filter condition: %w", err)
			}
			return ok.Data.(bool), nil
		})
	}

	// At this state a stream of rows turns into a strem of row groups.
	// If the group by clause is present, the groups are formed according to
	// it. If not, each row is converted to its own group - so that the
	// projection step could work uniformly.
	groupsStream, err := groupRows(input, Q)
	if err != nil {
		return nil, err
	}
	if len(Q.OrderBy) > 0 {
		groupsStream, err = orderRows(groupsStream, Q)
		if err != nil {
			return nil, err
		}
	}
	if Q.Limit.Set {
		groupsStream = groupsStream.limit(Q.Limit.Value)
	}
	return project(groupsStream, Q), nil
}

func groupRows(input *Stream[Row], Q Query) (*Stream[[]Row], error) {
	if len(Q.GroupBy) == 0 {
		return groupByNothing(input, Q)
	}

	allRows, err := input.Consume()
	if err != nil {
		return nil, err
	}
	keyeq := func(a, b []Value) bool {
		for i := range a {
			if a[i].Data != b[i].Data {
				return false
			}
		}
		return true
	}
	groups := [][]Row{}
	groupKeys := [][]Value{}
	for _, row := range allRows {
		key := []Value{}
		for _, e := range Q.GroupBy {
			ev, err := eval(e, row, nil)
			if err != nil {
				panic(err)
			}
			key = append(key, ev)
		}

		// find the bucket
		i := -1
		for j := range groupKeys {
			if keyeq(groupKeys[j], key) {
				i = j
				break
			}
		}
		if i < 0 {
			i = len(groupKeys)
			groupKeys = append(groupKeys, key)
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], row)
	}
	return arrstream(groups), nil
}

func groupByNothing(input *Stream[Row], Q Query) (*Stream[[]Row], error) {
	hasExpressions := false
	hasAggregates := false
	for _, x := range Q.Selectors {
		switch x.Expr.(type) {
		case *aggregate:
			hasAggregates = true
		case *columnRef, *functionkek, *Value, *star:
			hasExpressions = true
		default:
			panic(fmt.Errorf("unhandled switch case: %s", reflect.TypeOf(x.Expr)))
		}
	}

	// select id, count(*)
	if hasExpressions && hasAggregates {
		return nil, fmt.Errorf("can't use field expressions with aggregations without group by")
	}

	// select id
	if hasExpressions {
		return mapStream(input, func(r Row) ([]Row, error) {
			return []Row{r}, nil
		}), nil
	}
	// select count(*)
	init := false
	return &Stream[[]Row]{
		"all rows as one group " + FormatQuery(Q),
		func() ([]Row, bool, error) {
			if init {
				return nil, true, nil
			}
			init = true
			rows, err := input.Consume()
			if err != nil {
				return nil, false, err
			}
			return rows, false, nil
		}}, nil
}

func concatRows(a, b Row) Row {
	r := make(Row, len(a)+len(b))
	i := 0
	for _, cell := range a {
		r[i] = cell
		i++
	}
	for _, cell := range b {
		r[i] = cell
		i++
	}
	return r
}

func joinTables(xs, ys *Stream[Row]) *Stream[Row] {
	var err error
	var left, right Row
	var leftdone bool
	var rightdone bool

	init := false
	yss, rewind := rewindable(ys)

	advance := func() {
		if !init {
			init = true
			left, leftdone, err = xs.Next()
		}
		if err != nil {
			return
		}
		if leftdone {
			return
		}
		right, rightdone, err = yss.Next()
		if err != nil {
			return
		}
		if rightdone {
			yss = rewind()
			right, rightdone, err = yss.Next()
			if err != nil {
				return
			}
			left, leftdone, err = xs.Next()
		}
	}

	return &Stream[Row]{
		fmt.Sprintf("join(%s,%s)", xs.name, ys.name),
		func() (Row, bool, error) {
			advance()
			if err != nil {
				return nil, false, err
			}
			if leftdone || rightdone {
				return nil, true, nil
			}
			return concatRows(left, right), false, nil
		},
	}
}

func orderRows(s *Stream[[]Row], q Query) (*Stream[[]Row], error) {
	groups, err := s.Consume()
	if err != nil {
		return nil, err
	}
	result := make([][]Row, len(groups))
	copy(result, groups)
	sort.Slice(result, func(i, j int) bool {
		for _, ordering := range q.OrderBy {
			v1, err := eval(ordering.expr, result[i][0], result[i])
			if err != nil {
				panic(err)
			}
			v2, err := eval(ordering.expr, result[j][0], result[j])
			if err != nil {
				panic(err)
			}
			if v1.Data == v2.Data {
				continue
			}
			if v1.Data == nil {
				return false
			}
			if v2.Data == nil {
				return true
			}
			less, err := v1.lessThan(v2)
			if err != nil {
				panic(err)
			}
			if ordering.desc {
				return !less
			}
			return less
		}
		return false
	})
	return arrstream(result), nil
}

func project(s *Stream[[]Row], Q Query) *Stream[Row] {
	return mapStream(s, func(group []Row) (Row, error) {
		var exampleRow Row
		if len(group) > 0 {
			exampleRow = group[0]
		}
		groupRow := make(Row, 0)
		for _, selector := range Q.Selectors {
			// Expand star selectors with full rows
			if _, ok := selector.Expr.(*star); ok {
				for _, c := range exampleRow {
					groupRow = append(groupRow, Cell{Name: c.Name, Data: c.Data})
				}
				continue
			}
			val, err := eval(selector.Expr, exampleRow, group)
			if err != nil {
				return nil, err
			}
			alias := selector.Alias
			if alias == "" {
				alias = selector.Expr.String()
			}
			groupRow = append(groupRow, Cell{Name: alias, Data: val})
		}
		return groupRow, nil
	})
}
