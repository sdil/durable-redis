package main

import (
	"context"
	"go.etcd.io/etcd/client/v3"
)

func watchEtcd(etcd *clientv3.Client) {
	ctx, _ := context.WithCancel(context.Background())
	
		ch := etcd.Watch(ctx, "/some/keyspace", clientv3.WithPrefix())
		for resp := range ch {
			for _, event := range resp.Events {
				logger.Info(event.Kv.String())
			}
		}
}