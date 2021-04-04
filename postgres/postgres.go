package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/docker/go-connections/nat"
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
	env      map[string]string
	c        testcontainers.Container
	host     string
	port     int
	mainConn *sql.DB

	temporaryFiles []string
	lock           sync.Mutex
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

func New(creq ContainerRequest) (*Container, func(), error) {
	return NewCtx(context.Background(), creq)
}

func NewCtx(ctx context.Context, creq ContainerRequest) (*Container, func(), error) {
	initScripts, tempfile, err := migrations(creq.InitScripts)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	container := &Container{
		c:              postgresC,
		env:            creq.Env,
		temporaryFiles: []string{tempfile},
		lock:           sync.Mutex{},
	}
	teardown := func() {
		for _, f := range container.temporaryFiles {
			os.Remove(f)
		}

		_ = postgresC.Terminate(ctx)
	}

	container.host, err = postgresC.Host(ctx)
	if err == nil {
		var port nat.Port

		port, err = postgresC.MappedPort(ctx, natPort)
		if err == nil {
			container.port = port.Int()
			container.mainConn, err = sql.Open("postgres", container.DSN())
		}
	}

	if err != nil {
		teardown()

		return nil, nil, err
	}

	return container, teardown, nil
}

type Options struct {
	PrefixName  string
	InitScripts InitScripts
}

func (c *Container) CreateDatabse(opts Options) (string, error) {
	name := fmt.Sprintf("%s_%d", opts.PrefixName, time.Now().UnixNano())

	initScripts, tempfile, err := migrations(opts.InitScripts)
	if err != nil {
		return "", err
	}

	c.lock.Lock()
	c.temporaryFiles = append(c.temporaryFiles, tempfile)
	c.lock.Unlock()

	_, err = c.mainConn.Exec("create database " + name)
	if err != nil {
		return "", err
	}

	dsn := c.dsn(name)
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return "", err
	}

	defer conn.Close()

	for file := range initScripts {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return "", err
		}

		_, err = conn.Exec(string(data))
		if err != nil {
			return "", err
		}
	}

	return dsn, nil
}

func (c *Container) DSN() string {
	return c.dsn(c.env[envDB])
}

func (c *Container) dsn(database string) string {
	return fmt.Sprintf(dsnTemplate, dbUser, c.env[envPass], c.host, c.port, database)
}

func migrations(i InitScripts) (map[string]string, string, error) {
	var (
		ret      = make(map[string]string)
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
