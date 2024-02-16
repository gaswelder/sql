package sql

import (
	"fmt"
	"strings"
)

type tokenType string

const (
	tEnd        tokenType = "end"
	tString               = "string"
	tIdentifier           = "identifier"
	tNumber               = "number"
	tKeyword              = "keyword"
	tOp                   = "operator"
	tError                = "error"
)

type token struct {
	t   tokenType
	val string
}

func (t token) String() string {
	return fmt.Sprintf("[%s %s]", t.t, t.val)
}

type tokenizer struct {
	b     *Parsebuf
	peeks []token
}

func (tr *tokenizer) unget(t token) {
	tr.peeks = append(tr.peeks, t)
}

func (tr *tokenizer) peek() token {
	s, err := tr.next()
	if err != nil {
		return token{tError, err.Error()}
	}
	if s.t != tEnd {
		tr.unget(s)
	}
	return s
}

var operators = []string{
	"=", "*", ".", "[", "]", "(", ")", ",", "<", ">",
}
var keywords = []string{
	"select", "as", "from", "join", "on", "where", "order", "group", "by", "limit",
	"desc", "asc",
	"or", "and",
	"array", "true", "false",
	"int",
}

func (tr *tokenizer) next() (token, error) {
	if len(tr.peeks) > 0 {
		r := tr.peeks[len(tr.peeks)-1]
		tr.peeks = tr.peeks[0 : len(tr.peeks)-1]
		return r, nil
	}
	tr.b.Space()
	if tr.b.Peek() == "" {
		return token{tEnd, ""}, nil
	}
	if tr.b.Peek() == "'" {
		s, err := readQuote(tr.b, "'")
		if err != nil {
			panic(err)
		}
		return token{tString, s}, nil
	}
	if tr.b.Peek() == "\"" {
		s, err := readQuote(tr.b, "\"")
		if err != nil {
			panic(err)
		}
		return token{tIdentifier, s}, nil
	}
	if tr.b.Peek() == "-" {
		tr.b.Get()
		if tr.b.Peek()[0] >= '0' && tr.b.Peek()[0] <= '9' {
			s := "-" + tr.b.Set("0123456789")
			return token{tNumber, s}, nil
		}
	}
	if tr.b.Peek()[0] >= '0' && tr.b.Peek()[0] <= '9' {
		s := tr.b.Set("0123456789")
		return token{tNumber, s}, nil
	}
	for _, s := range operators {
		if tr.b.Peek() == s {
			tr.b.Get()
			return token{tOp, s}, nil
		}
	}

	s := tr.b.Set("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
	if s == "" {
		return token{}, fmt.Errorf("unexpected trailing string: %s", tr.b.Rest())
	}
	for _, tok := range keywords {
		if strings.ToLower(s) == tok {
			return token{tKeyword, strings.ToUpper(s)}, nil
		}
	}
	return token{tIdentifier, s}, nil
}

func (tr *tokenizer) eat(t tokenType, val string) bool {
	p := tr.peek()
	if p.t == t && p.val == val {
		tr.next()
		return true
	}
	return false
}

func (tr *tokenizer) eati(t tokenType, val string) bool {
	p := tr.peek()
	if p.t == t && strings.ToLower(p.val) == strings.ToLower(val) {
		tr.next()
		return true
	}
	return false
}

func readQuote(b *Parsebuf, q string) (string, error) {
	if !b.Literal(q) {
		return "", fmt.Errorf("'%s' expected", q)
	}
	s := strings.Builder{}
	for b.More() {
		if b.Literal("\\") {
			c := b.Get()
			s.WriteString(c)
			continue
		}
		if b.Peek() == q {
			break
		}
		c := b.Get()
		s.WriteString(c)
	}
	if !b.Literal(q) {
		return s.String(), fmt.Errorf("'%s' expected", q)
	}
	return s.String(), nil
}
