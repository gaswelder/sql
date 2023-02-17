package sql

import (
	"fmt"
	"strings"
)

func normalize(q *Query, tables map[string]Table) error {
	// Collect table names used in the query.
	tablesInUse := []string{}
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
			tablesInUse = append(tablesInUse, n)
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
			for _, tbl := range tablesInUse {
				table := tables[tbl]
				for _, col := range table.ColumnNames() {
					q.Selectors = append(q.Selectors, selector{expr: &columnRef{tbl, col}})
				}
			}
			continue
		}
		q.Selectors = append(q.Selectors, s)
	}
	return nil
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
