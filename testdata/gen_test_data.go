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
				CASE WHEN i % 11 = 0 THEN CAST('NaN' AS DOUBLE)
				     WHEN i % 7 = 0 THEN NULL
				     ELSE round(((i * 37) % 10000)::DOUBLE / 100, 2) END AS value,
				CASE WHEN i % 10 = 0 THEN NULL
				     WHEN i % 3 = 0 THEN 'A'
				     WHEN i % 3 = 1 THEN 'B'
				     ELSE 'C' END AS category,
				CASE WHEN i % 15 = 0 THEN CAST('NaN' AS DOUBLE)
				     WHEN i % 6 = 0 THEN NULL
				     ELSE ((i * 17) % 101)::DOUBLE END AS score,
				CASE WHEN i % 5 = 0 THEN NULL
				     ELSE current_date - ((i % 365)::INTEGER) END AS created_date,
				i % 2 = 0 AS active
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
				round(((i * 23) % 5000)::DOUBLE / 100, 2) AS value,
				CASE WHEN i % 9 = 0 THEN NULL ELSE 'cat_' || (i % 5)::VARCHAR END AS category
			FROM generate_series(1, 50) t(i)
		) TO 'testdata/sample.csv' (FORMAT CSV, HEADER);
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate csv: %v\n", err)
		os.Exit(1)
	}

	_, err = db.Exec(`
		COPY (
			SELECT * FROM (
				VALUES
					(
						1,
						'compact_object',
						'{"event":"signup","user":{"id":101,"plan":"pro"},"tags":["alpha","beta"],"active":true}',
						'Short plain-text row for raw reader comparison.'
					),
					(
						2,
						'long_object',
						'{"event":"page_view","path":"/atlas/studies/session/42","body":"This payload is intentionally long so the expanded reader has something real to format and pan across.","metrics":{"duration_ms":1834,"scroll_depth":0.82,"retry_count":2},"flags":[true,false,null],"notes":["first","second","third"]}',
						'This is an intentionally long plain string column entry to compare wrap behavior against the JSON payload column in the expanded reader.'
					),
					(
						3,
						'json_array',
						'[{"step":"extract","ok":true},{"step":"transform","ok":true},{"step":"load","ok":false,"reason":"network timeout"}]',
						'Another regular string row with no JSON semantics.'
					),
					(
						4,
						'invalid_json',
						'{"broken": true',
						'Invalid JSON in payload should stay in raw mode.'
					),
					(
						5,
						'plain_text',
						'This payload column is just prose, not JSON. It should open in raw mode and still support row-to-row reader navigation.',
						'Final plain string value.'
					)
			) AS t(id, scenario, payload, description)
		) TO 'testdata/json_reader_sample.parquet' (FORMAT PARQUET);
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate json reader parquet: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generated testdata/sample.parquet, testdata/sample.csv, and testdata/json_reader_sample.parquet")
}
