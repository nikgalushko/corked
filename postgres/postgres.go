package postgres

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	image   = "postgres:12.4-alpine"
	port    = "5432/tcp"
	natPort = "5432"
)

const (
	dbName = "postgres"
	dbUser = "postgres"
	dbPass = "password"

	dsnTemplate = "postgres://%s:%s@%s:%d/%s?sslmode=disable"
)

const (
	envDB   = "POSTGRES_DB"
	envPass = "POSTGRES_PASSWORD"
)

type Container struct {
	t *testing.T
	c testcontainers.Container
}

type InitScripts struct {
	FromDir   string
	FromFiles []string
	Inline    string
}

type ContainerRequest struct {
	testcontainers.GenericContainerRequest
	InitScripts InitScripts
}

var defaultEnv = map[string]string{
	envDB:   dbName,
	envPass: dbPass,
}

func New(creq ContainerRequest) (Container, func(), error) {
	return NewCtx(context.Background(), creq)
}

func NewCtx(ctx context.Context, creq ContainerRequest) (Container, func(), error) {
	initScripts, tempfile, err := migrations(creq.InitScripts)
	if err != nil {
		return Container{}, nil, err
	}

	if creq.Image == "" {
		creq.Image = image
	}

	creq.ExposedPorts = []string{port}
	creq.Env = merge(defaultEnv, creq.Env)
	creq.BindMounts = merge(initScripts, creq.BindMounts)
	creq.WaitingFor = wait.NewHostPortStrategy(natPort)

	postgresC, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: creq.ContainerRequest,
			Started:          true,
		},
	)
	if err != nil {
		return Container{}, nil, err
	}

	return Container{c: postgresC}, func() {
		os.Remove(tempfile)
		postgresC.Terminate(ctx)
	}, nil
}

func (c Container) DSN() string {
	return c.DSNCtx(context.Background())
}

func (c Container) DSNCtx(ctx context.Context) string {
	host, err := c.c.Host(ctx)
	if err != nil {
		c.t.Fatal(err)
	}

	port, err := c.c.MappedPort(ctx, natPort)
	if err != nil {
		c.t.Fatal(err)
	}

	return fmt.Sprintf(dsnTemplate, dbUser, dbPass, host, port.Int(), dbName)
}

func migrations(i InitScripts) (map[string]string, string, error) {
	var (
		ret      map[string]string
		tempfile string
	)

	if i.Inline != "" {
		filename, err := createTempFileWithContent("init.sql", i.Inline)
		if err != nil {
			return nil, "", err
		}

		ret = map[string]string{
			filename: "/docker-entrypoint-initdb.d/init.sql",
		}
		tempfile = filename
	} else if len(i.FromFiles) != 0 {
		for _, f := range i.FromFiles {
			if !filepath.IsAbs(f) {
				return nil, "", fmt.Errorf("filepath should be absoulte but %s", f)
			}

			ret[f] = "/docker-entrypoint-initdb.d/" + filepath.Base(f)
		}
	} else if i.FromDir != "" {
		if !filepath.IsAbs(i.FromDir) {
			return nil, "", fmt.Errorf("filepath should be absoulte but %s", i.FromDir)
		}

		ret[i.FromDir] = "/docker-entrypoint-initdb.d"
	}

	return ret, tempfile, nil
}

func merge(a, b map[string]string) map[string]string {
	ret := make(map[string]string, len(a))

	for k, v := range a {
		ret[k] = v
	}

	for k, v := range b {
		ret[k] = v
	}

	return ret
}

func createTempFileWithContent(filename, content string) (string, error) {
	tmpfile, err := ioutil.TempFile("", filename)
	if err != nil {
		return "", err
	}

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		tmpfile.Close()

		return "", err
	}

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}
