package etcdclient

import (
	"github.com/etcd-io/etcd/clientv3"
	"github.com/ytinirt/doggie/pkg/util"
	"fmt"
	etcdclientv3 "github.com/etcd-io/etcd/clientv3"
	"time"
	"strings"
)

type EtcdClient struct {
	endpoint string
	caFile string
	certFile string
	keyFile string
	client *clientv3.Client
}

func endpointValid(ep string) (err error) {
	prefix := "https://"
	if !strings.HasPrefix(ep, prefix) {
		return fmt.Errorf("not has prefix https://")
	}

	return nil
}

func New(endpoint, caFile, certFile, keyFile string) (client *EtcdClient, err error) {
	if err := endpointValid(endpoint); err != nil {
		return nil, fmt.Errorf("invalid endpoint %s, %v", endpoint, err)
	}

	files := []string{caFile, certFile, keyFile}
	for _, f := range files {
		exists, err := util.FileExists(f)
		if !exists {
			if err == nil {
				return nil, fmt.Errorf("missing file %s", f)
			} else {
				return nil, fmt.Errorf("meet problem while finding file %s, %v", f, err)
			}
		}
	}

	tlsConfig, err := util.GenTLSConfig(endpoint, caFile, certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("generate TLS config failed, %v", err)
	}

	cli, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{endpoint},
		DialTimeout: 10 * time.Second,
		TLS: tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("create Etcd client failed, %v", err)
	}

	client = &EtcdClient{
		endpoint: endpoint,
		caFile: caFile,
		certFile: certFile,
		keyFile: keyFile,
		client: cli,
	}
	return client, nil
}