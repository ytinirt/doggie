package etcdclient

import (
	"github.com/ytinirt/doggie/pkg/util"
	"github.com/etcd-io/etcd/clientv3"
	"fmt"
	"strings"
	"github.com/ytinirt/doggie/pkg/log"
	"time"
	"context"
)

type EtcdClient struct {
	ep string
	caFile string
	certFile string
	keyFile string
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

	client = &EtcdClient{
		ep: endpoint,
		caFile: caFile,
		certFile: certFile,
		keyFile: keyFile,
	}
	return client, nil
}

func (ec *EtcdClient) serverName() string {
	str := strings.TrimPrefix(ec.ep, "https://")
	str = strings.TrimLeft(str, "/")
	idx := strings.IndexByte(str, ':')
	str = str[:idx]

	return str
}

func (ec *EtcdClient) IsLeader() bool {
	tlsConfig, err := util.GenTLSConfig(ec.serverName(), ec.caFile, ec.certFile, ec.keyFile)
	if err != nil {
		log.Error("generate TLS config failed, %v", err)
		return false
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ec.ep},
		DialTimeout: 10 * time.Second,
		TLS: tlsConfig,
	})
	if err != nil {
		log.Error("create Etcd client failed, %v", err)
		return false
	}
	defer cli.Close()

	resp, err := cli.Status(context.Background(), ec.ep)
	if err != nil {
		log.Error("query etcd endpoint status failed, %v", err)
		return false
	}

	isLeader := resp.Header.MemberId == resp.Leader
	log.Debug("endpoint member id %d, leader id %d", resp.Header.MemberId, resp.Leader)

	return isLeader
}
