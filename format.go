package sql

import (
	"fmt"
	"reflect"
	"sort"
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

func FormatRowsAsTable(results []Row, width int) string {
	var header []string
	for _, cell := range results[0] {
		header = append(header, cell.Name)
	}

	stringRows := [][]string{header}
	for _, row := range results {
		srow := []string{}
		for _, h := range header {
			for _, cell := range row {
				if cell.Name == h {
					srow = append(srow, fmt.Sprintf("%v", cell.Data.Data))
				}
			}
		}
		stringRows = append(stringRows, srow)
	}
	return asciiTable(stringRows, true, width)
}

func FormatRowsAsList(rr []Row) string {
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

func asciiTable(table [][]string, header bool, width int) string {
	// Column index -> column widths.
	// Initialize each column to fit its widest cell.
	colWidths := map[int][]int{}
	colsNumber := len(table[0])
	for colIndex := 0; colIndex < colsNumber; colIndex++ {
		for rowIndex := 0; rowIndex < len(table); rowIndex++ {
			cell := table[rowIndex][colIndex]
			colWidths[colIndex] = append(colWidths[colIndex], len(cell))
		}
		sort.Ints(colWidths[colIndex])
	}
	fmt.Println(colWidths)

	p := func(xs []int, perc float64) int {
		return xs[int(float64(len(xs)-1)*perc)]
	}

	colWidth := map[int]int{}
	for r := 1.0; r >= 0.5; r -= 0.1 {
		tableWidth := 1 // 1 for the left table border
		for i := 0; i < colsNumber; i++ {
			colWidth[i] = p(colWidths[i], r)
			// space content space border
			tableWidth += 1 + colWidth[i] + 1 + 1
		}
		fmt.Println(r, tableWidth, width)
		if tableWidth <= width {
			break
		}
	}

	lineb := strings.Builder{}
	lineb.WriteString("+ ")
	for i := range table[0] {
		w := colWidth[i]
		lineb.WriteString(strings.Repeat("-", w))
		lineb.WriteString(" + ")
	}
	lineb.WriteString("\n")

	row := func(cells []string) string {
		b := strings.Builder{}
		b.WriteString("|")
		for i, title := range cells {
			w := colWidth[i]
			l := len(title)

			var padded string
			if l <= w {
				padded = title + strings.Repeat(" ", w-l)
			} else {
				padded = title[0:w-3] + strings.Repeat(".", 3)
			}
			b.WriteString(" ")
			b.WriteString(padded)
			b.WriteString(" |")
		}
		b.WriteString("\n")
		return b.String()
	}

	b := strings.Builder{}
	if header {
		b.WriteString(lineb.String())
		b.WriteString(row(table[0]))
		b.WriteString(lineb.String())
		table = table[1:]
	}
	for _, r := range table {
		b.WriteString(row(r))
	}
	b.WriteString(lineb.String())
	return b.String()
}
