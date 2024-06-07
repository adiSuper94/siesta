# siesta
## Intoduction
Siesta genarates **type-safe** REST APIs directly from your postgres DB instance 🐘

## Installation
Siesta requires Go 1.22+

`go install github.com/adisupe94/siesta`

## Usage
- Create your database schema anyway you like.
- Run siesta to generate type-safe code to access your database create http handlers.
- In your main func, import the `GetRouter` and call it with the postgres connection/pool. Hook it up to your http server like this
    ```go
    router := GetRouter(db)
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	log.Fatal(httpServer.ListenAndServe())
    ```

