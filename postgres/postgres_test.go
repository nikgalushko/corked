package postgres

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestNew_Simple(t *testing.T) {
	c, teardown, err := New(ContainerRequest{})
	assert.NoError(t, err)

	defer teardown()

	db, err := sql.Open("postgres", c.DSN())
	assert.NoError(t, err)

	var (
		actualVersion   string
		expectedVersion = "PostgreSQL 12.4 on x86_64-pc-linux-musl, compiled by gcc (Alpine 9.3.0) 9.3.0, 64-bit"
	)

	row := db.QueryRow("select version()")
	err = row.Scan(&actualVersion)
	assert.NoError(t, err)

	assert.Equal(t, expectedVersion, actualVersion)
}

func TestNew_Migrations_Inline(t *testing.T) {
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
	assert.NoError(t, err)

	defer teardown()

	assert.Equal(t, map[string]struct{}{
		"names":  {},
		"cities": {},
	}, selectTables(t, c.DSN()))
}

func TestNew_Migrations_Files(t *testing.T) {
	file, err := filepath.Abs("./testdata/migrations/init.up.sql")
	assert.NoError(t, err)

	c, teardown, err := New(ContainerRequest{
		InitScripts: InitScripts{
			FromFiles: []string{file},
		},
	})
	assert.NoError(t, err)

	defer teardown()

	assert.Equal(t, map[string]struct{}{
		"names":  {},
		"cities": {},
	}, selectTables(t, c.DSN()))
}

func TestNew_Migrations_FilesIsNotAbs(t *testing.T) {
	c, teardown, err := New(ContainerRequest{
		InitScripts: InitScripts{
			FromFiles: []string{"./testdata/migrations/init.up.sql"},
		},
	})
	assert.Nil(t, teardown)
	assert.Error(t, err)
	assert.Empty(t, c)
}

func selectTables(t *testing.T, dsn string) map[string]struct{} {
	db, err := sql.Open("postgres", dsn)
	assert.NoError(t, err)

	tables := make(map[string]struct{})
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	assert.NoError(t, err)

	defer rows.Close()

	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		assert.NoError(t, err)

		tables[tableName] = struct{}{}
	}

	return tables
}
