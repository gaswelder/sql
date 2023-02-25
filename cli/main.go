package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/gaswelder/sql"
)

func rowToJSON(r sql.Row) (string, error) {
	m := map[string]any{}
	for _, c := range r {
		n := c.Name
		if _, ok := m[n]; ok {
			i := 0
			for {
				i++
				n = fmt.Sprintf("%s_%d", c.Name, i)
				if _, ok := m[n]; !ok {
					break
				}
			}
		}
		m[n] = c.Data.Data
	}
	data, err := json.Marshal(m)
	return string(data), err
}

var formatters = map[string]func([]sql.Row){
	"j": func(rows []sql.Row) {
		for _, r := range rows {
			j, err := rowToJSON(r)
			if err != nil {
				panic(err)
			}
			fmt.Println(j)
		}
	},
	"t": func(rows []sql.Row) {
		fmt.Println(sql.FormatRowsAsTable(rows, 100))
	},
}

func main() {
	outputFormat := flag.String("f", "t", "output format (t = table, j = json)")
	flag.Parse()
	args := flag.Args()
	format := formatters[*outputFormat]
	if format == nil {
		os.Stderr.WriteString(fmt.Sprintf("unknown output format: '%s'\n", *outputFormat))
		os.Exit(1)
	}
	if len(args) != 2 {
		os.Stderr.WriteString(fmt.Sprintf("usage: %s <json-file> <query>\n", os.Args[0]))
		os.Exit(1)
	}

	Q, err := sql.Parse(args[1])
	if err != nil {
		panic(err)
	}

	if args[0] == "-" {
		reader := bufio.NewReader(os.Stdin)
		next := func() (map[string]any, error) {
			line, err := reader.ReadString('\n')
			if err != nil {
				return nil, err
			}
			var m map[string]any
			err = json.Unmarshal([]byte(line), &m)
			if err != nil {
				return nil, err
			}
			return m, nil
		}

		firstRow, err := next()
		if err != nil {
			panic(err)
		}
		items := make(chan map[string]any)
		e := sql.New(map[string]sql.Table{"t": &input{
			firstRow: firstRow,
			items:    items,
		}})
		go func() {
			items <- firstRow
			for {
				row, err := next()
				if err == io.EOF {
					close(items)
					return
				}
				if err != nil {
					panic(err)
				}
				items <- row
			}
		}()
		rows, err := e.Exec(Q)
		if err != nil {
			panic(err)
		}
		for {
			r, done, err := rows.Next()
			if err != nil {
				panic(err)
			}
			if done {
				break
			}
			s, err := rowToJSON(r)
			if err != nil {
				panic(err)
			}
			fmt.Println(s)
		}
	} else {
		table := sql.JsonTable(args[0])
		e := sql.New(map[string]sql.Table{"t": table})
		rows, err := e.ExecString(args[1])
		if err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		format(rows)
	}
}

type input struct {
	firstRow map[string]any
	items    chan map[string]any
}

func (i *input) ColumnNames() []string {
	r := []string{}
	for k := range i.firstRow {
		r = append(r, k)
	}
	return r
}

func (i *input) GetRows() func() (map[string]sql.Value, error) {
	return func() (map[string]sql.Value, error) {
		item, ok := <-i.items
		if !ok {
			return nil, nil
		}
		row := map[string]sql.Value{}
		for k := range i.firstRow {
			row[k] = sql.Value{Type: sql.String, Data: item[k]}
		}
		return row, nil
	}
}
