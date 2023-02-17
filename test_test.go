package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestQueries(t *testing.T) {
	data := map[string]Table{
		"cars": jsonTable("test-data.json"),
		"t1": dummy{
			{"id": Value{Int, 1}, "name": Value{String, "one"}},
			{"id": Value{Int, 2}, "name": Value{String, "'"}},
			{"id": Value{Int, 3}, "name": Value{String, "three"}},
		},
		"t2": dummy{
			{"bucket": Value{Int, 1}},
			{"bucket": Value{Int, 2}},
			{"bucket": Value{Int, 2}},
		},
		"t3": dummy{
			{"x": Value{Int, 1}},
			{"x": Value{Int, 2}},
		},
		"a-b": dummy{
			{"x": Value{Int, 1}},
		},
	}

	mp := func(rr []map[string]any) string {
		sb := strings.Builder{}
		for _, r := range rr {
			i := 0
			for k, v := range r {
				if i > 0 {
					sb.WriteString(", ")
				}
				i++
				sb.WriteString(fmt.Sprintf("%s=%v", k, v))
			}
			sb.WriteByte('\n')
		}
		return sb.String()
	}

	engine := New(data)
	check := func(name, query string, want []map[string]any) {
		t.Run(name, func(t *testing.T) {
			r, err := engine.ExecString(query)
			if err != nil {
				fmt.Println("query: " + query)
				t.Fatal(err)
			}
			diff := cmp.Diff(rowsAsJSON(r), want)
			if diff != "" {
				fmt.Printf("query:\n%s\n\nwant:\n%s\ngot:\n%s\n", query, mp(want), formatRows(r))
				t.Fatalf("%s", diff)
			}
		})
	}

	check("simplest projection", `select name from cars`, []map[string]any{
		{"\"name\"": "BMW Z4 Roadster (II)"},
		{"\"name\"": "Cadillac SRX"},
		{"\"name\"": "Kia Soul"},
	})
	check("quotted field name", `select id, "name" from t1`, []map[string]any{
		{"\"id\"": 1, "\"name\"": "one"},
		{"\"id\"": 2, "\"name\"": "'"},
		{"\"id\"": 3, "\"name\"": "three"},
	})
	check("case-insensitive column and table names", `select ID, Name from T1`, []map[string]any{
		{`"ID"`: 1, `"Name"`: "one"},
		{`"ID"`: 2, `"Name"`: "'"},
		{`"ID"`: 3, `"Name"`: "three"},
	})
	check("simplest filter", `select id from t1 where name = '\''`, []map[string]any{
		{"\"id\"": 2},
	})
	check("simplest count", `select count(*) from t1`, []map[string]any{
		{"count(*)": 3},
	})
	check("simplest order", `select id from t1 order by id desc`, []map[string]any{
		{"\"id\"": 3},
		{"\"id\"": 2},
		{"\"id\"": 1},
	})
	check("order with limit", `select id from t1 order by "id" desc limit 1`, []map[string]any{
		{`"id"`: 3},
	})
	check("simplest star", `select * from t1`, []map[string]any{
		{`id`: 1, `name`: "one"},
		{`id`: 2, `name`: "'"},
		{`id`: 3, `name`: "three"},
	})
	check("star with join", `select * from t1 join t2 on id = bucket`, []map[string]any{
		{`id`: 1, `name`: "one", `bucket`: 1},
		{`id`: 2, `name`: "'", `bucket`: 2},
		{`id`: 2, `name`: "'", `bucket`: 2},
	})
	check("funky table name", `select * from "a-b"`, []map[string]any{
		{`x`: 1},
	})
	check("bare select", `select 1`, []map[string]any{
		{"1": 1},
	})
	check("group", `select bucket, count(*) from t2 group by bucket order by count(*) desc`, []map[string]any{
		{`"bucket"`: 2, "count(*)": 2},
		{`"bucket"`: 1, "count(*)": 1},
	})
	check("limit", `select bucket from t2 limit 2`, []map[string]any{
		{`"bucket"`: 1},
		{`"bucket"`: 2},
	})
	check("min", `select year, min(price) from cars group by year`, []map[string]any{
		{`"year"`: 2009, `min("price")`: 30000},
		{`"year"`: 2005, `min("price")`: 69000},
	})

	check("...", `select bucket, x from t2 join t3 on array_contains(array[1,2,3], 1)`, []map[string]any{
		{"\"bucket\"": 1, `"x"`: 1},
		{"\"bucket\"": 1, `"x"`: 2},
		{"\"bucket\"": 2, `"x"`: 1},
		{"\"bucket\"": 2, `"x"`: 2},
		{"\"bucket\"": 2, `"x"`: 1},
		{"\"bucket\"": 2, `"x"`: 2},
	})
	check("order by count", `select "bucket", count(*) from t2 group by "bucket" order by count(*) desc limit 1`, []map[string]any{
		{`"bucket"`: 2, "count(*)": 2},
	})
	check("simplest alias", `select "bucket" as b from t2`, []map[string]any{
		{`b`: 1},
		{`b`: 2},
		{`b`: 2},
	})
	check("subquery", `select * from (select year from cars)`, []map[string]any{
		{`"year"`: 2009},
		{`"year"`: 2005},
		{`"year"`: 2009},
	})
	check("nulls last asc", `select name, weight from cars order by weight`, []map[string]any{
		{`"name"`: "Cadillac SRX", `"weight"`: 1950},
		{`"name"`: "BMW Z4 Roadster (II)", `"weight"`: nil},
		{`"name"`: "Kia Soul", `"weight"`: nil},
	})
	check("nulls last desc", `select name, weight from cars order by weight desc`, []map[string]any{
		{`"name"`: "Cadillac SRX", `"weight"`: 1950},
		{`"name"`: "BMW Z4 Roadster (II)", `"weight"`: nil},
		{`"name"`: "Kia Soul", `"weight"`: nil},
	})
	check("filter", `select year from cars where year < 2009`, []map[string]any{
		{`"year"`: 2005},
	})
	// check("filter with nulls", `select year from cars where year < 2009`, []map[string]any{
	// 	{`"year"`: 2005},
	// })
}

func rowsAsJSON(rr []Row) []map[string]any {
	var result []map[string]any
	for _, row := range rr {
		item := map[string]any{}
		for _, cell := range row {
			item[cell.Name] = cell.Data.Data
		}
		result = append(result, item)
	}
	return result
}
