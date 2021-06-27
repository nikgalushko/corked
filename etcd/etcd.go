package etcd

import (
	"context"
	"fmt"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	image   = "bitnami/etcd:3"
	port    = "2379/tcp"
	natPort = "2379"
)

type Container struct {
	c    testcontainers.Container
	host string
	port int
}

type InitScripts struct {
	Inline map[string]string
}

type ContainerRequest struct {
	testcontainers.GenericContainerRequest
	InitScripts InitScripts
}

func New(creq ContainerRequest) (*Container, func(), error) {
	return NewCtx(context.Background(), creq)
}

func NewCtx(ctx context.Context, creq ContainerRequest) (*Container, func(), error) {
	if creq.Image == "" {
		creq.Image = image
	}

	creq.ExposedPorts = []string{port}
	creq.Env = merge(map[string]string{"ALLOW_NONE_AUTHENTICATION": "yes"}, creq.Env)
	creq.WaitingFor = wait.NewHostPortStrategy(natPort)

	etcdC, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: creq.ContainerRequest,
			Started:          true,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	container := &Container{c: etcdC}
	teardown := func() {
		_ = etcdC.Terminate(ctx)
	}

	container.host, err = etcdC.Host(ctx)
	if err == nil {
		var port nat.Port

		port, err = etcdC.MappedPort(ctx, natPort)
		if err == nil {
			container.port = port.Int()
		}
	}

	if err != nil {
		teardown()

		return nil, nil, err
	}

	return container, teardown, nil
}

func (c Container) URL() string {
	return fmt.Sprintf("%s:%d", c.host, c.port)
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
