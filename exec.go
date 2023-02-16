package sql

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/pkg/errors"
)

// Row is a collection of cells.
type Row []Cell

func (r Row) get(table, name string) Value {
	for _, cell := range r {
		if cell.TableName == table && cell.Name == name {
			return cell.Data
		}
	}
	panic(fmt.Sprintf("couldn't find %s.%s in a row", table, name))
}

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

type RowsStream struct {
	Next func() (Row, error)
}

func (s *RowsStream) Consume() ([]Row, error) {
	return consume(s.Next)
}

// Exec runs the query and returns the results.
func (e Engine) Exec(Q Query) (*RowsStream, error) {
	if err := normalize(&Q, e.tables); err != nil {
		return nil, err
	}

	// Stream the input
	var input func() (Row, error)
	if Q.From != nil {
		table, ok := e.tables[Q.From.Name]
		if !ok {
			return nil, fmt.Errorf("table not found: %s", Q.From.Name)
		}
		input = tableIter(Q.From.Name, table.GetRows())
	} else {
		input = arrayIterator([]Row{{}})
	}

	// Join other inputs
	for _, j := range Q.Joins {
		table, ok := e.tables[j.Table.Name]
		if !ok {
			return nil, fmt.Errorf("table not found: %s", j.Table.Name)
		}
		more := tableIter(j.Table.Name, table.GetRows())
		input = joinTablesFilter(input, more, j.Condition)
	}

	// Filter the stream
	if Q.Filter != nil {
		input = filter(Q, input)
	}

	// Group the stream
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

	// Format the results
	results := project(groupsStream, Q)
	return toRowsStream(results), nil
}

func filter(Q Query, input func() (Row, error)) func() (Row, error) {
	return func() (Row, error) {
		for {
			r, err := input()
			if r == nil || err != nil {
				return r, err
			}
			ok, err := Q.Filter.eval(r, nil)
			if err != nil {
				return nil, errors.Wrap(err, "failed to calculate filter condition")
			}
			if ok.Data.(bool) {
				return r, nil
			}
		}
	}
}

func project(groupsIt *stream[[]Row], Q Query) *stream[Row] {
	return conv(groupsIt, func(rows []Row) (Row, error) {
		exampleRow := rows[0]
		groupRow := make(Row, 0)
		for _, selector := range Q.Selectors {
			val, err := selector.expr.eval(exampleRow, rows)
			if err != nil {
				return nil, err
			}
			alias := selector.alias
			if alias == "" {
				alias = selector.expr.String()
			}
			groupRow = append(groupRow, Cell{Name: alias, Data: val})
		}
		return groupRow, nil
	})
}

func groupRows(input func() (Row, error), Q Query) (*stream[[]Row], error) {
	if len(Q.GroupBy) == 0 {
		s, err := groupByNothing(input, Q)
		if err != nil {
			return nil, err
		}
		return toStream2(s), nil
	}

	filtered, err := consume(input)
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
	for _, row := range filtered {
		key := []Value{}
		for _, e := range Q.GroupBy {
			ev, err := e.eval(row, nil)
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

func groupByNothing(input func() (Row, error), Q Query) (func() ([]Row, error), error) {
	hasExpressions := false
	hasAggregates := false
	for _, x := range Q.Selectors {
		if _, ok := x.expr.(*aggregate); ok {
			hasAggregates = true
		} else {
			hasExpressions = true
		}
	}
	// select id, count(*)
	if hasExpressions && hasAggregates {
		return nil, fmt.Errorf("can't use field expressions with aggregations without group by")
	}

	// select id
	if hasExpressions {
		return func() ([]Row, error) {
			r, err := input()
			if r == nil || err != nil {
				return nil, err
			}
			return []Row{r}, nil
		}, nil

	}
	// select count(*)
	init := false
	return func() ([]Row, error) {
		if init {
			return nil, nil
		}
		init = true
		rows, err := consume(input)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}, nil
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

func joinTables(xs, ys func() (Row, error)) func() (Row, error) {
	var err error
	var left, right Row

	init := false
	yss, rewind := rewindable(ys)

	advance := func() {
		if !init {
			init = true
			left, err = xs()
		}
		if err != nil {
			return
		}
		if left == nil {
			return
		}
		right, err = yss()
		if err != nil {
			return
		}
		if right == nil {
			rewind()
			right, err = yss()
			if err != nil {
				return
			}
			left, err = xs()
		}
	}

	return func() (Row, error) {
		advance()
		if err != nil {
			return nil, err
		}
		if left == nil || right == nil {
			return nil, nil
		}
		return concatRows(left, right), nil
	}
}

func joinTablesFilter(xs, ys func() (Row, error), condition expression) func() (Row, error) {
	join := joinTables(xs, ys)
	return func() (Row, error) {
		for {
			r, err := join()
			if err != nil || r == nil {
				return r, err
			}
			ev, err := condition.eval(r, nil)
			if err != nil {
				return nil, err
			}
			if ev.Data.(bool) {
				return r, nil
			}
		}
	}
}

func consumeGroups(groupsIt func() ([]Row, error)) ([][]Row, error) {
	var groups [][]Row
	for {
		r, err := groupsIt()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		groups = append(groups, r)
	}
	return groups, nil
}

func orderRows(groupsIt *stream[[]Row], q Query) (*stream[[]Row], error) {
	groups, err := groupsIt.consume()
	if err != nil {
		return nil, err
	}
	result := make([][]Row, len(groups))
	copy(result, groups)
	sort.Slice(result, func(i, j int) bool {
		for _, ordering := range q.OrderBy {
			v1, err := ordering.expr.eval(result[i][0], result[i])
			if err != nil {
				panic(err)
			}
			v2, err := ordering.expr.eval(result[j][0], result[j])
			if err != nil {
				panic(err)
			}
			if ordering.desc {
				v1, v2 = v2, v1
			}
			if v1.Data == v2.Data {
				continue
			}
			switch a := v1.Data.(type) {
			case int:
				return a < v2.Data.(int)
			case string:
				return a < v2.Data.(string)
			default:
				panic(fmt.Errorf("don't know how to compare %s %v", reflect.TypeOf(v1.Data), v1.Data))
			}
		}
		return false
	})
	return arrstream(result), nil
}
