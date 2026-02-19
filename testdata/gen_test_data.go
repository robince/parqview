//go:build ignore

package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	_, err = db.Exec(`
		COPY (
			SELECT
				i AS id,
				'item_' || i::VARCHAR AS name,
				round(random() * 100, 2) AS value,
				CASE WHEN random() < 0.1 THEN NULL
				     WHEN random() < 0.4 THEN 'A'
				     WHEN random() < 0.7 THEN 'B'
				     ELSE 'C' END AS category,
				CASE WHEN random() < 0.15 THEN NULL
				     ELSE (random() * 100)::INTEGER END AS score,
				CASE WHEN random() < 0.2 THEN NULL
				     ELSE current_date - (random() * 365)::INTEGER END AS created_date,
				random() < 0.5 AS active
			FROM generate_series(1, 200) t(i)
		) TO 'testdata/sample.parquet' (FORMAT PARQUET);
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate: %v\n", err)
		os.Exit(1)
	}

	// Also a CSV
	_, err = db.Exec(`
		COPY (
			SELECT
				i AS id,
				'item_' || i::VARCHAR AS name,
				round(random() * 100, 2) AS value,
				CASE WHEN random() < 0.1 THEN NULL ELSE 'cat_' || (random()*5)::INTEGER::VARCHAR END AS category
			FROM generate_series(1, 50) t(i)
		) TO 'testdata/sample.csv' (FORMAT CSV, HEADER);
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate csv: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generated testdata/sample.parquet and testdata/sample.csv")
}
