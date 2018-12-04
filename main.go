package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/lib/pq"
	"google.golang.org/api/option"
)

const Version = "1.2.0"
const CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"

var (
	pgConn      = flag.String("uri", "postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable", "postgres connection uri")
	pgSchema    = flag.String("schema", "public", "postgres schema")
	pgTable     = flag.String("table", "", "postgres table name")
	datasetId   = flag.String("dataset", "", "BigQuery dataset")
	projectId   = flag.String("project", "", "BigQuery project id")
	partitions  = flag.Int("partitions", -1, "Number of per-day partitions, -1 to disable")
	versionFlag = flag.Bool("version", false, "Print program version")
	exclude     = flag.String("exclude", "", "columns to exclude")
	ignoreTypes = flag.Bool("ignore-unknown-types", false, "Ignore unknown column types")
)

type Column struct {
	Name       string
	Type       string
	IsNullable string
}

func (c *Column) ToFieldSchema() (*bigquery.FieldSchema, error) {
	var f bigquery.FieldSchema
	f.Name = c.Name
	f.Required = c.IsNullable == "NO"

	switch c.Type {
	case "varchar", "bpchar", "text", "citext", "xml", "cidr", "inet", "uuid", "bit", "varbit", "bytea", "money":
		f.Type = bigquery.StringFieldType
	case "int2", "int4", "int8":
		f.Type = bigquery.IntegerFieldType
	case "float4", "float8", "numeric":
		f.Type = bigquery.FloatFieldType
	case "bool":
		f.Type = bigquery.BooleanFieldType
	case "timestamptz":
		f.Type = bigquery.TimestampFieldType
	case "date":
		f.Type = bigquery.DateFieldType
	case "timestamp":
		f.Type = bigquery.DateTimeFieldType
	case "time":
		f.Type = bigquery.TimeFieldType
	default:
		return nil, errors.New("Unknown column type: " + c.Type)
	}

	return &f, nil
}

func schemaFromPostgres(db *sql.DB, schema, table string) bigquery.Schema {
	rows, err := db.Query(`SELECT column_name, udt_name, is_nullable FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	excludes := strings.Split(*exclude, ",")
	var c Column
	var s bigquery.Schema
	for rows.Next() {
		if err := rows.Scan(&c.Name, &c.Type, &c.IsNullable); err != nil {
			log.Fatal(err)
		}
		if !contains(c.Name, excludes) {
			f, err := c.ToFieldSchema()
			if err == nil {
				s = append(s, f)
			} else if !*ignoreTypes {
				panic(err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return s
}

func contains(s string, haystack []string) bool {
	for i := 0; i < len(haystack); i++ {
		if s == haystack[i] {
			return true
		}
	}
	return false
}

func columnsFromSchema(schema bigquery.Schema) string {
	cols := make([]string, len(schema))
	for i, field := range schema {
		cols[i] = pq.QuoteIdentifier(field.Name)
	}
	return strings.Join(cols, ",")
}

func getRowsStream(db *sql.DB, schema bigquery.Schema, pgSchema, table string) io.Reader {
	rows, err := db.Query(fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT %s FROM %s.%s) AS t`, columnsFromSchema(schema), pq.QuoteIdentifier(pgSchema), pq.QuoteIdentifier(table)))
	if err != nil {
		log.Fatal(err)
	}
	reader, writer := io.Pipe()
	go func() {
		defer rows.Close()
		defer writer.Close()
		for rows.Next() {
			var b []byte
			rows.Scan(&b)
			writer.Write(b)
			writer.Write([]byte{'\n'})
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}()
	return reader
}

func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	if *versionFlag {
		fmt.Fprintf(os.Stderr, "%s version: %s (%s on %s/%s; %s)\n", os.Args[0], Version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.Compiler)
		os.Exit(0)
	}
	keyfile := os.Getenv(CREDENTIALS)
	if keyfile == "" {
		log.Fatal("!! missing ", CREDENTIALS)
	}
	opt := option.WithServiceAccountFile(keyfile)
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *projectId, opt)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", *pgConn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	partitioned := *partitions > -1

	schema := schemaFromPostgres(db, *pgSchema, *pgTable)
	table := client.Dataset(*datasetId).Table(*pgTable)
	if _, err := table.Metadata(ctx); err != nil {
		metadata := &bigquery.TableMetadata{Schema: schema}
		if partitioned {
			metadata.TimePartitioning = &bigquery.TimePartitioning{
				Expiration: time.Duration(*partitions) * 24 * time.Hour,
			}
		}
		if err := table.Create(ctx, metadata); err != nil {
			log.Fatal(err)
		}
	}

	if partitioned {
		table.TableID += time.Now().UTC().Format("$20060102")
	}

	f := getRowsStream(db, schema, *pgSchema, *pgTable)
	rs := bigquery.NewReaderSource(f)
	rs.SourceFormat = bigquery.JSON
	rs.MaxBadRecords = 0
	rs.Schema = schema
	loader := table.LoaderFrom(rs)
	loader.CreateDisposition = bigquery.CreateNever
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for {
		status, err := job.Status(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if status.Statistics.Details != nil {
			details := status.Statistics.Details.(*bigquery.LoadStatistics)
			log.Println("OutputBytes", details.OutputBytes)
			log.Println("OutputRows", details.OutputRows)
		}
		if status.Done() {
			if status.Err() != nil {
				log.Fatal(status.Err())
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

}
