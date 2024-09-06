package startup

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewEtcdConn(address string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: 5 * time.Second,
	})
}
