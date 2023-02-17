package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("usage: %s <json-file> <query>\n", os.Args[0])
		os.Exit(1)
	}
	table := jsonTable(os.Args[1])
	e := New(map[string]Table{
		"t": table,
	})
	rows, err := e.ExecString(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

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
			data, err := json.Marshal(m)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(data))
		}
	}

}
