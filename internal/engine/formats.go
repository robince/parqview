package engine

import (
	"fmt"
	"path/filepath"
	"strings"
)

type dataSourceFormat struct {
	extensions []string
	readerExpr string
}

var supportedDataSourceFormats = []dataSourceFormat{
	{extensions: []string{".parquet"}, readerExpr: "read_parquet"},
	{extensions: []string{".csv"}, readerExpr: "read_csv_auto"},
	{extensions: []string{".json"}, readerExpr: "read_json_auto"},
	{extensions: []string{".jsonl", ".ndjson"}, readerExpr: "read_ndjson_auto"},
}

// SupportedExtensions returns the extensions recognized by the engine.
func SupportedExtensions() []string {
	var exts []string
	for _, format := range supportedDataSourceFormats {
		exts = append(exts, format.extensions...)
	}
	return exts
}

// IsSupportedDataFile reports whether the file extension is recognized by the engine.
func IsSupportedDataFile(path string) bool {
	_, ok := sourceFormatForPath(path)
	return ok
}

func sourceExprForPath(path string) (string, error) {
	format, ok := sourceFormatForPath(path)
	if !ok {
		return "", fmt.Errorf("unsupported file extension: %s", strings.ToLower(filepath.Ext(path)))
	}

	// v1 keeps DuckDB reader selection extension-based and auto-detect only.
	// .json uses the regular JSON reader, .jsonl/.ndjson use the NDJSON reader,
	// and nested LIST/STRUCT types are preserved as-is.
	return fmt.Sprintf("%s('%s')", format.readerExpr, escapeSQLString(path)), nil
}

func sourceFormatForPath(path string) (dataSourceFormat, bool) {
	ext := strings.ToLower(filepath.Ext(path))
	for _, format := range supportedDataSourceFormats {
		for _, candidate := range format.extensions {
			if ext == candidate {
				return format, true
			}
		}
	}
	return dataSourceFormat{}, false
}
