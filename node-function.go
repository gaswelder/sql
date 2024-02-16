package sql

type functionkek struct {
	name string
	args []expression
}

func (f functionkek) eval(r Row, group []Row) (Value, error) {
	args := make([]Value, len(f.args))
	for i, argExpression := range f.args {
		exprResult, err := argExpression.eval(r, group)
		if err != nil {
			return exprResult, err
		}
		args[i] = exprResult
	}

	return function(f.name, args)
}
