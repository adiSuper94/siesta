package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/adisuper94/siesta/generated"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func Test1(t *testing.T) {
	db := createDBConnection(2)
	router := generated.GetRouter(db)
	defer db.Close()
	server := httptest.NewServer(router)
	defer server.Close()
	var url string
	url = fmt.Sprintf("%s/messages", server.URL)
	res, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error while call GET(all) endpoint for messages : \nurl: %s\nerror: %v\n", url, err)
		t.Fail()
	}
	body, err := io.ReadAll(res.Body)
  var prettyJSON bytes.Buffer
  err = json.Indent(&prettyJSON, body,"", "  ")
	if res.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "response status is not OK.\nresponse status:%d\n body:%s", res.StatusCode, body)
	}
	fmt.Println(string(prettyJSON.Bytes()))
}
