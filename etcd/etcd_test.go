package etcd

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestSimple(t *testing.T) {
	c, teardown, err := New(ContainerRequest{})
	if err != nil {
		log.Fatal(err)
	}

	defer teardown()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{c.URL()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	defer cli.Close()

	_, err = cli.Put(context.Background(), "/key", "blah")
	require.NoError(t, err)

	resp, err := cli.Get(context.Background(), "/key")
	require.NoError(t, err)

	require.Equal(t, int64(1), resp.Count)
	for _, kv := range resp.Kvs {
		require.Equal(t, "/key", string(kv.Key))
		require.Equal(t, "blah", string(kv.Value))
	}
}
