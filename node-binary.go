package sql

type binaryOperatorNode struct {
	op    string
	left  expression
	right expression
}

type fbinaryOr struct {
	left  expression
	right expression
}
