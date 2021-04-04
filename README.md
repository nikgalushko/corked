# corked
 Useful helpers for testing based on testcontainers-go

## 🔨 How to use
### Postgres
To create a simple container with a database, just use this snippet:
```go
c, teardown, err := New(ContainerRequest{})
if err != nil {
	t.Fatal(err)
}

defer teardown()
```
This creates a database based on the image postgres:12.4-alpine with a database "postgres", the user "postgres" and the password "password". Now you can easily get a DSN and start writing tests.
```go
db, err := sql.Open("postgres", c.DSN())
if err != nil {
	t.Fatal(err)
}

rows, err := db.Query(...)
if err != nil {
    t.Fatal(err)
}

defer rows.Close()

for rows.Next() {
    ...
}
```

What database can do without migrations? You can simply create a database with migrations:
```go
c, teardown, err := New(ContainerRequest{
	InitScripts: InitScripts{
		Inline: `
			begin;
			/* Your sql is here. */
			commit;
		`,
	},
})
if err != nil {
	t.Fatal(err)
}

defer teardown()
```
Migrations can also be from multiple files `InitScripts.FromFiles` or even from a directory `InitScripts.FromDir`.
Migration priorities: `Inline` > `FromFiles` > `FromDir`.
