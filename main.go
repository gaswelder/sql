package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

var formatters = map[string]func([]Row){
	"j": func(rows []Row) {
		for _, r := range rows {
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
			if err != nil {
				panic(err)
			}
			fmt.Println(string(data))
		}
	},
	"t": func(rows []Row) {
		fmt.Println(formatRows(rows))
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

	table := jsonTable(args[0])
	e := New(map[string]Table{"t": table})
	rows, err := e.ExecString(args[1])
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
	format(rows)
}
