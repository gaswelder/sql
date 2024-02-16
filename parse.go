package sql

import (
	"fmt"
	"strconv"
)

// Parse parses an SQL string and returns a query syntax tree.
func Parse(sqlString string) (Query, error) {
	b := tokenizer{
		b:     NewParsebuf(sqlString),
		peeks: nil,
	}
	result, err := readQuery(&b)
	if err != nil {
		return result, err
	}
	if b.peek().t != tEnd {
		fmt.Println(FormatQuery(result))
		return result, fmt.Errorf("unexpected token: %s", b.peek())
	}
	return result, nil
}

func readQuery(b *tokenizer) (Query, error) {
	var result Query
	if !b.eati(tKeyword, "SELECT") {
		return result, fmt.Errorf("SELECT expected, got %s", b.peek())
	}
	for {
		e, err := readSelector(b)
		if err != nil {
			return result, err
		}
		result.Selectors = append(result.Selectors, e)
		if !b.eat(tOp, ",") {
			break
		}
	}
	if b.eati(tKeyword, "FROM") {
		if b.eati(tOp, "(") {
			// Subquery
			q, err := readQuery(b)
			if err != nil {
				return result, err
			}
			if !b.eati(tOp, ")") {
				return result, fmt.Errorf("expected ')' after subquery, got %s", b.peek())
			}
			result.From = fromspec{Kind: kindSubquery, Q: &q}
		} else {
			// Table name
			from, err := b.next()
			if err != nil {
				return result, err
			}
			if from.t != tIdentifier {
				return result, fmt.Errorf("expected identifier, got %s", from)
			}
			result.From = fromspec{Kind: kindTableName, Tn: &tableName{from.val}}
		}
		result.Joins = readJoins(b)
	}
	if b.eati(tKeyword, "WHERE") {
		var err error
		result.Filter, err = readExpression(b)
		if err != nil {
			return result, err
		}
	}
	if b.eati(tKeyword, "GROUP") {
		if !b.eati(tKeyword, "BY") {
			return result, fmt.Errorf("expected BY after GROUP, got '%s", b.peek())
		}
		for {
			gr, err := readExpression(b)
			if err != nil {
				return result, err
			}
			result.GroupBy = append(result.GroupBy, gr)
			if !b.eat(tOp, ",") {
				break
			}
		}
	}
	if b.eati(tKeyword, "ORDER") {
		if !b.eati(tKeyword, "BY") {
			return result, fmt.Errorf("expected BY after ORDER, got '%s", b.peek())
		}
		for {
			result.OrderBy = append(result.OrderBy, readOrder(b))
			if !b.eat(tOp, ",") {
				break
			}
		}
	}
	if b.eati(tKeyword, "LIMIT") {
		n, err := b.next()
		if err != nil {
			return result, err
		}
		if n.t != tNumber {
			return result, fmt.Errorf("expecting a number after LIMIT, got %s", n)
		}
		val, err := strconv.Atoi(n.val)
		if err != nil {
			return result, err
		}
		result.Limit.Set = true
		result.Limit.Value = val
	}
	return result, nil
}

func readOrder(b *tokenizer) orderspec {
	expr, err := readExpression(b)
	if err != nil {
		panic(err)
	}
	desc := false
	switch true {
	case b.eat(tKeyword, "DESC"):
		desc = true
	case b.eat(tKeyword, "ASC"):
		//
	}
	return orderspec{desc, expr}
}

func readSelector(b *tokenizer) (selector, error) {
	if b.eat(tOp, "*") {
		return selector{Expr: &star{}, Alias: ""}, nil
	}
	expr, err := readExpression(b)
	if err != nil {
		return selector{}, err
	}
	if b.eati(tKeyword, "as") {
		alias, err := b.next()
		if err != nil {
			return selector{}, err
		}
		if alias.t != tIdentifier {
			return selector{}, fmt.Errorf("expected identifier after AS, got %s", alias)
		}
		return selector{Expr: expr, Alias: alias.val}, nil
	}
	return selector{Expr: expr}, nil
}

func readJoins(b *tokenizer) []joinspec {
	var r []joinspec
	for b.eat(tKeyword, "JOIN") {
		table, err := b.next()
		if err != nil {
			panic(err)
		}
		if table.t != tIdentifier {
			panic(fmt.Errorf("expected identifier, got %s", table))
		}
		if !b.eat(tKeyword, "ON") {
			panic(fmt.Errorf("expected ON, got '%s'", b.peek()))
		}
		condition, err := readExpression(b)
		if err != nil {
			panic(err)
		}
		r = append(r, joinspec{&tableName{table.val}, condition})
	}
	return r
}

func readExpression(b *tokenizer) (expression, error) {
	e, err := readExpr1(b)
	if err != nil {
		return nil, err
	}
	for b.eati(tKeyword, "OR") {
		e2, err := readExpr1(b)
		if err != nil {
			return nil, err
		}
		e = &fbinaryOr{e, e2}
	}
	return e, nil
}

func readExpr1(b *tokenizer) (expression, error) {
	e, err := readExpr0(b)
	if err != nil {
		return nil, err
	}
	for _, op := range []string{"=", "<", ">"} {
		if b.eat(tOp, op) {
			e2, err := readExpr0(b)
			if err != nil {
				return nil, err
			}
			return &binaryOperatorNode{op, e, e2}, nil
		}
	}
	return e, nil
}

func readExpr0(b *tokenizer) (expression, error) {
	if scalar, err := readScalar(b); scalar != nil || err != nil {
		return scalar, err
	}
	if b.eati(tKeyword, "TRUE") {
		return &Value{Bool, true}, nil
	}
	if b.eati(tKeyword, "FALSE") {
		return &Value{Bool, false}, nil
	}
	if b.eati(tKeyword, "ARRAY") {
		if !b.eat(tOp, "[") {
			return nil, fmt.Errorf("[ expected, got %s", b.peek())
		}
		var array []Value
		for {
			item, err := readScalar(b)
			if err != nil {
				return nil, err
			}
			if item == nil {
				break
			}
			array = append(array, *item)
			if !b.eat(tOp, ",") {
				break
			}
		}
		if !b.eat(tOp, "]") {
			return nil, fmt.Errorf("] expected, got %s", b.peek())
		}
		return &Value{Array, array}, nil
	}

	name1, err := b.next()
	if err != nil {
		return nil, err
	}
	if name1.t != tIdentifier {
		return nil, fmt.Errorf("identifier expected, got %s", name1)
	}

	if b.peek().t == tOp && b.peek().val == "(" && isAggregate(name1.val) {
		b.next()
		args := []expression{}
		if b.eat(tOp, "*") {
			args = append(args, &star{})
			if !b.eat(tOp, ")") {
				return nil, fmt.Errorf("expecting ) after %s(*", name1.val)
			}
		} else {
			for {
				e, err := readExpression(b)
				if err != nil {
					return nil, err
				}
				args = append(args, e)
				if !b.eat(tOp, ",") {
					break
				}
			}
			if !b.eat(tOp, ")") {
				return nil, fmt.Errorf(") expected, got %s", b.peek())
			}
		}
		return &aggregate{name1.val, args}, nil
	}

	if b.eat(tOp, "(") {
		args := []expression{}
		for {
			e, err := readExpression(b)
			if err != nil {
				return nil, err
			}
			args = append(args, e)
			if !b.eat(tOp, ",") {
				break
			}
		}
		if !b.eat(tOp, ")") {
			return nil, fmt.Errorf(") expected, got %s", b.peek())
		}
		return &functionkek{name1.val, args}, nil
	}

	if b.eat(tOp, ".") {
		name2, err := b.next()
		if err != nil {
			return nil, err
		}
		if name2.t != tIdentifier {
			return nil, fmt.Errorf("identifier expected, got %s", name2)
		}
		return &columnRef{name1.val, name2.val}, nil
	}

	return &columnRef{"", name1.val}, nil
}

func readScalar(b *tokenizer) (*Value, error) {
	if b.peek().t == tString {
		s, err := b.next()
		if err != nil {
			return nil, err
		}
		return &Value{String, s.val}, nil
	}
	if b.peek().t == tNumber {
		s, err := b.next()
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(s.val)
		if err != nil {
			return nil, err
		}
		return &Value{Int, n}, nil
	}
	return nil, nil
}
