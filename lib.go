package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func New(db DBTX) *pgAdapter {
	return &pgAdapter{conn: db}
}

type pgAdapter struct {
	conn   DBTX
	dbName string
	schema []string
}

type pgSchema struct {
	tableCatalog string
	tableSchema  string
	Tables       map[string]pgTable
}

type pgTable struct {
	TableType string
	TableName string
	Columns   map[string]pgColumn
	PK        map[string]pgColumn
}

type pgColumn struct {
	ColumnName    string
	columnDefault sql.NullString
	isNullable    sql.NullString
	dataType      string
	isGenerated   sql.NullString
}

func intSlice(start int, end int) []int {
	if start >= end {
		return nil
	}
	result := make([]int, end-start+1)
	for i := start; i <= end; i++ {
		result[i-start] = i
	}
	return result
}

func (db pgAdapter) scanDB(ctx context.Context) {
	colQuery := `
  SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.column_default,
    c.is_nullable, c.data_type, c.is_generated, t.table_type
  FROM information_schema.columns as c
  INNER JOIN information_schema.tables as t
    ON t.table_schema = c.table_schema and t.table_name = c.table_name
  WHERE c.table_schema = 'public'`
	pkQuery := `
  SELECT tc."table_name", tc.table_schema, tc.table_catalog, tc.constraint_type, ccu."column_name"
  FROM "information_schema"."table_constraints" as tc
	INNER JOIN "information_schema"."constraint_column_usage" as ccu
	  ON tc."constraint_name" = ccu."constraint_name" AND
      tc."table_name" = ccu."table_name" AND tc.table_schema = ccu.table_schema
	WHERE tc.table_schema = 'public' AND tc.constraint_type = 'PRIMARY KEY'`

	catalog := make(map[string]pgSchema)
	columns, err := db.conn.Query(ctx, colQuery)
	if err != nil {
		log.Fatalf(" error while reading columns %v", err)
	}
	pks, err := db.conn.Query(ctx, pkQuery)
	if err != nil {
		log.Fatalf("error while reading pks %v", err)
	}
	for columns.Next() {
		var catalogName, tableSchema, tableName, columnName, columnDefault, isNullable, dataType, isGenerated, tableType sql.NullString
		columns.Scan(&catalogName, &tableSchema, &tableName, &columnName, &columnDefault, &isNullable, &dataType, &isGenerated, &tableType)
		if !catalogName.Valid || !tableSchema.Valid || !tableName.Valid || !columnName.Valid || !tableType.Valid || !dataType.Valid {
			continue
		}
		schema, ok := catalog[tableSchema.String]
		if !ok {
			schema = pgSchema{
				tableCatalog: catalogName.String,
				tableSchema:  tableSchema.String,
				Tables:       make(map[string]pgTable),
			}
			catalog[tableSchema.String] = schema
		}
		table, ok := schema.Tables[tableName.String]
		if !ok {
			table = pgTable{
				TableName: tableName.String,
				TableType: tableType.String,
				Columns:   make(map[string]pgColumn),
				PK:        make(map[string]pgColumn),
			}
			schema.Tables[tableName.String] = table
		}
		column := pgColumn{
			ColumnName:    columnName.String,
			columnDefault: columnDefault,
			isNullable:    isNullable,
			dataType:      dataType.String,
			isGenerated:   isGenerated,
		}
		table.Columns[columnName.String] = column
		if err != nil {
			log.Fatalf(" error while iterating tables %v", err)
		}
	}
	for pks.Next() {
		var tableName, tableSchema, tableCatalog, constraintType, columnName sql.NullString
		pks.Scan(&tableName, &tableSchema, &tableCatalog, &constraintType, &columnName)
		if !tableCatalog.Valid || !tableSchema.Valid || !tableName.Valid || !columnName.Valid || !constraintType.Valid {
			continue
		}
		schema, ok := catalog[tableSchema.String]
		if !ok {
			continue
		}
		table, ok := schema.Tables[tableName.String]
		if !ok {
			continue
		}
		pkColumn, ok := table.Columns[columnName.String]
		if !ok {
			continue
		}
		table.PK[columnName.String] = pkColumn

	}
	fmt.Printf("\n\n\n")
	tmpl, err := template.New("model.templ").
		Funcs(template.FuncMap{"toCamelCase": strcase.ToCamel, "intSlice": intSlice}).
		ParseFiles("model.templ")
	if err != nil {
		log.Fatalf("errror while parsing template %v", err)
	}
	for _, schema := range catalog {
		tmpl.Execute(os.Stdout, schema)
	}
}

func (db pgAdapter) getAll(ctx context.Context, schemaName string, table pgTable) []interface{} {
	query := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, schemaName, table.TableName)
	fmt.Printf("%s\n", query)
	rows, err := db.conn.Query(ctx, query)
	defer rows.Close()
	fd := rows.FieldDescriptions()
	fmt.Printf("%v\n", fd)
	if err != nil {
		log.Fatalf(" error while executing getAll for %s \n %v", table.TableName, err)
	}
	var result []interface{}
	cols := table.Columns
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil
		}
		m := make(map[string]interface{})
		i := 0
		for colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = *val
			i += 1
		}
		fmt.Printf("%v", m)
	}
	return result
}

func (column pgColumn) GetGoType() string {
	if !column.isNullable.Valid {
		log.Fatalf("column nullable information not available in column %v", column)
	}
	switch column.dataType {
	case "uuid", "text", "character varying":
		if column.isNullable.String == "NO" {
			return "string"
		} else {
			return "sql.NullString"
		}
	case "boolean":
		if column.isNullable.String == "NO" {
			return "bool"
		} else {
			return "sql.NullBool"
		}
	case "timestamp with time zone":
		return "pgtype.Timestamptz"
	}
	log.Fatalf("invalid dataype for column: %v", column)
	return ""
}

func createDBConnection(connectionCount int32) *pgxpool.Pool {
	pgxConfig, err := pgxpool.ParseConfig("postgres://adisuper:adisuper@localhost:5432/turbo?sslmode=disable")
	if err != nil {
		panic(err)
	}
	pgxConfig.MaxConns = connectionCount

	conn, err := pgxpool.NewWithConfig(context.TODO(), pgxConfig)
	if err != nil {
		panic(err)
	}
	return conn
}

func main() {
	var connectionCount int32
	connectionCount = 2
	conn := createDBConnection(connectionCount)
	defer conn.Close()
	pg := New(conn)
	pg.scanDB(context.Background())

}
