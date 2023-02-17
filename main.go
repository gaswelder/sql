package main

import (
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
		for i, c := range r {
			if i > 0 {
				fmt.Printf(" | ")
			}
			fmt.Printf("%s=%s", c.Name, c.Data.String())
		}
		fmt.Printf("\n")
	}
}
