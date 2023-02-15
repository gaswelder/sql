package sql

import (
	"fmt"
	"strings"
)

func normalize(q *Query, tables map[string]Table) error {
	// First normalize table names
	tablesMap := map[string]bool{}
	err := traverse(q, func(node any) error {
		if tbl, ok := node.(*tableName); ok {
			if tbl == nil {
				return nil
			}
			n, err := canonicalTableName(tables, tbl.Name)
			if err != nil {
				return err
			}
			if n == "" {
				return fmt.Errorf("could not resolve table name '%s'", tbl.Name)
			}
			tbl.Name = n
			tablesMap[n] = true
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Replace stars
	var selects = q.Selectors
	q.Selectors = []selector{}
	for _, s := range selects {
		if _, ok := s.expr.(*star); ok {
			for tbl := range tablesMap {
				table := tables[tbl]
				for _, col := range table.ColumnNames() {
					q.Selectors = append(q.Selectors, selector{expr: &columnRef{tbl, col}})
				}
			}
			continue
		}
		q.Selectors = append(q.Selectors, s)
	}

	// Normalize column refs using normalized table names as hints.
	var possibleTables []string
	for k := range tablesMap {
		possibleTables = append(possibleTables, k)
	}
	return traverse(q, func(node any) error {
		if ref, ok := node.(*columnRef); ok {
			t, f, err := canonicalColumnName(tables, ref.Table, ref.Column, possibleTables)
			if err != nil {
				return err
			}
			if t == "" || f == "" {
				return fmt.Errorf("could not resolve column %s", ref.String())
			}
			ref.Table = t
			ref.Column = f
		}
		return nil
	})
}

func canonicalTableName(tables map[string]Table, tbl string) (string, error) {
	var matches []string
	for t := range tables {
		if idMatch(t, tbl) {
			matches = append(matches, t)
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("couldn't find type that matches '%s'", tbl)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("ambiguous type \"%s\": %s", tbl, strings.Join(matches, ", "))
	}
	return matches[0], nil
}

func canonicalColumnName(tables map[string]Table, tbl, col string, possibleTables []string) (string, string, error) {
	var matches = [][2]string{}
	for _, t := range possibleTables {
		if tbl != "" && !idMatch(t, tbl) {
			continue
		}
		possibleColumns := tables[t].ColumnNames()
		for _, c := range possibleColumns {
			if idMatch(c, col) {
				matches = append(matches, [2]string{t, c})
			}
		}
	}
	if len(matches) == 0 {
		return tbl, col, fmt.Errorf("couldn't find table and column that match %s.%s", tbl, col)
	}
	if len(matches) > 1 {
		return tbl, col, fmt.Errorf("ambiguous reference %s.%s: %v", tbl, col, matches)
	}
	return matches[0][0], matches[0][1], nil
}

func idMatch(full, x string) bool {
	if strings.Contains(full, "/") {
		return nsMatch(full, x)
	}
	return strings.EqualFold(full, x)
}

func nsMatch(full, x string) bool {
	fullParts := strings.Split(full, "/")
	xParts := strings.Split(x, "/")
	if len(xParts) == 1 {
		return strings.EqualFold(xParts[0], fullParts[1])
	}
	if len(xParts) == 2 {
		return strings.EqualFold(xParts[0], fullParts[0]) && strings.EqualFold(xParts[1], fullParts[1])
	}
	panic("couldn't parse " + full)
}
