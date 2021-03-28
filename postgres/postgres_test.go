package postgres

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestNew_Simple(t *testing.T) {
	c, teardown, err := New(ContainerRequest{})
	if err != nil {
		t.Fatal(err)
	}

	defer teardown()

	db, err := sql.Open("postgres", c.DSN())
	if err != nil {
		t.Fatal(err)
	}

	var (
		actualVersion   string
		expectedVersion = "PostgreSQL 12.4 on x86_64-pc-linux-musl, compiled by gcc (Alpine 9.3.0) 9.3.0, 64-bit"
	)

	row := db.QueryRow("select version()")
	err = row.Scan(&actualVersion)
	if err != nil {
		t.Fatal(err)
	}

	if actualVersion != expectedVersion {
		t.Fatalf("selected version is not equal to expected; expected: %s\n; acutual: %s\n", expectedVersion, actualVersion)
	}
}

func TestNew_Migrations(t *testing.T) {
	c, teardown, err := New(ContainerRequest{
		InitScripts: InitScripts{
			Inline: `
				begin;
				create table names
				(
						name      varchar(36)           not null,
						processed boolean default false not null,
						id        serial                not null
				);

				create table cities
				(
						country    text,
						value      numeric     not null,
						start_date timestamp   not null,
						end_date   timestamp
				);
				commit;
			`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	defer teardown()

	db, err := sql.Open("postgres", c.DSN())
	if err != nil {
		t.Fatal(err)
	}

	tables := make(map[string]struct{})
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		tables[tableName] = struct{}{}
	}

	assert.Equal(t, map[string]struct{}{
		"names":  {},
		"cities": {},
	}, tables)
}
