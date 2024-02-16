package sql

import (
	"fmt"
	"strings"
)

// columnRef is an expression node that refers to a column.
type columnRef struct {
	Table  string
	Column string
}

func (e columnRef) eval(x Row, group []Row) (Value, error) {
	for _, cell := range x {
		if e.Table != "" && !strings.EqualFold(e.Table, cell.TableName) {
			continue
		}
		if !strings.EqualFold(e.Column, cell.Name) {
			continue
		}
		return cell.Data, nil
	}
	return Value{}, fmt.Errorf("couldn't find %s in a row", e)
}
