package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/gaswelder/sql"
)

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

	var f io.Reader
	if args[0] == "-" {
		f = os.Stdin
	} else {
		f, err := os.Open(args[0])
		if err != nil {
			panic(err)
		}
		defer f.Close()
	}
	e := sql.New(map[string]sql.Table{"t": sql.JsonStream(f)})
	rows, err := e.ExecString(args[1])
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
	format(rows)
}

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
