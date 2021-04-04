package postgres

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var newDatabse func(Options) (dsn string, err error)

func TestMain(m *testing.M) {
	c, teardown, err := New(ContainerRequest{})
	if err != nil {
		log.Fatal(err)
	}

	defer teardown()

	newDatabse = c.CreateDatabse

	code := m.Run()

	os.Exit(code)
}

func TestNew_Simple(t *testing.T) {
	dsn, err := newDatabse(Options{PrefixName: "simple"})
	assert.NoError(t, err)

	db, err := sql.Open("postgres", dsn)
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
	dsn, err := newDatabse(Options{
		PrefixName: "migrations_inline",
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

	assert.Equal(t, map[string]struct{}{
		"names":  {},
		"cities": {},
	}, selectTables(t, dsn))
}

func TestNew_Migrations_Files(t *testing.T) {
	file, err := filepath.Abs("./testdata/migrations/init.up.sql")
	assert.NoError(t, err)

	dsn, err := newDatabse(Options{
		PrefixName: "migrations_files",
		InitScripts: InitScripts{
			FromFiles: []string{file},
		},
	})

	assert.Equal(t, map[string]struct{}{
		"names":  {},
		"cities": {},
	}, selectTables(t, dsn))
}

func TestNew_Migrations_FilesIsNotAbs(t *testing.T) {
	dsn, err := newDatabse(Options{
		PrefixName: "migrations_files",
		InitScripts: InitScripts{
			FromFiles: []string{"./testdata/migrations/init.up.sql"},
		},
	})

	assert.Error(t, err)
	assert.Empty(t, dsn)
}

func TestNew_SpecialEnv(t *testing.T) {
	req := ContainerRequest{
		InitScripts: InitScripts{
			Inline: "",
		},
	}
	req.Env = map[string]string{
		"POSTGRES_PASSWORD": "super_secret_pass",
		"POSTGRES_DB":       "mydb",
	}
	c, teardown, err := New(req)
	assert.NoError(t, err)

	defer teardown()

	assert.Regexp(t, regexp.MustCompile(`postgres\:\/\/postgres\:super_secret_pass@localhost:(\d{1,5})\/mydb\?sslmode=disable`), c.DSN())
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
