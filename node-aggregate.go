package sql

import (
	"fmt"
)

type aggregate struct {
	name string
}

func (e aggregate) eval(rows []Row) Value {
	switch e.name {
	case "count(*)":
		return Value{Int, len(rows)}
	default:
		panic(fmt.Errorf("unknown aggregate: %s", e.name))
	}
}
func (e aggregate) String() string {
	return e.name
}
