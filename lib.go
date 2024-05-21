package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

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
	tables       map[string]pgTable
}

type pgTable struct {
	tableType string
	tableName string
	columns   map[string]pgColumn
	pk        map[string]pgColumn
}

type pgColumn struct {
	columnName    string
	columnDefault sql.NullString
	isNullable    sql.NullString
	dataType      string
	isGenerated   sql.NullString
}

func (db pgAdapter) getTablesNames(ctx context.Context) {
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
				tables:       make(map[string]pgTable),
			}
			catalog[tableSchema.String] = schema
		}
		table, ok := schema.tables[tableName.String]
		if !ok {
			table = pgTable{
				tableName: tableName.String,
				tableType: tableType.String,
				columns:   make(map[string]pgColumn),
				pk:        make(map[string]pgColumn),
			}
			schema.tables[tableName.String] = table
		}
		column := pgColumn{
			columnName:    columnName.String,
			columnDefault: columnDefault,
			isNullable:    isNullable,
			dataType:      dataType.String,
			isGenerated:   isGenerated,
		}
		table.columns[columnName.String] = column
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
		table, ok := schema.tables[tableName.String]
		if !ok {
			continue
		}
		pkColumn, ok := table.columns[columnName.String]
		if !ok {
			continue
		}
		table.pk[columnName.String] = pkColumn

	}
	fmt.Printf("%v", catalog)
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
	pg.getTablesNames(context.Background())

}
