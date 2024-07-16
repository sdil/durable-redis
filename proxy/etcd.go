package main

import (
	"context"
	"go.etcd.io/etcd/client/v3"
)

func WatchEtcd(etcd *clientv3.Client) {
	ctx, _ := context.WithCancel(context.Background())

	ch := etcd.Watch(ctx, "/proxy", clientv3.WithPrefix())
	for resp := range ch {
		for _, event := range resp.Events {
			if event.IsCreate() {
				logger.Info("New node registered", "node", event.Kv.Key)
				universe[string(event.Kv.Key)] = Node{Role: NodeRole(string(event.Kv.Value))}
			}

			if event.Type == clientv3.EventTypeDelete {
				logger.Info("Node unregistered", "node", event.Kv.Key)
				logger.Info(string(event.Kv.String()))
				delete(universe, string(event.Kv.Key))
			}

			logger.Info("World view", "nodes", universe)
		}
	}
}

func RegisterNode(etcd *clientv3.Client, ownIp string, node *Node) {
	key := "/proxy/" + ownIp
	_, err := etcd.Put(context.Background(), key, string(node.Role))
	if err != nil {
		logger.Error("Failed to register node", "err", err)
	}
}
