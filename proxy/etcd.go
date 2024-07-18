package main

import (
	"context"
	"fmt"
	"time"

	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	etcdKeyPrefix = "/proxy/"
)

func WatchPeerNodes(etcd *etcdclient.Client) {
	ctx, _ := context.WithCancel(context.Background())

	ch := etcd.Watch(ctx, etcdKeyPrefix, etcdclient.WithPrefix())
	for resp := range ch {
		for _, event := range resp.Events {
			ip := string(event.Kv.Key)[7:]
			if event.IsCreate() {
				logger.Info("New node registered", "node", ip)
				universe[ip] = Node{Role: NodeRole(string(event.Kv.Value)), IP: ip}
			}

			if event.Type == etcdclient.EventTypeDelete {
				logger.Info("Node unregistered", "node", ip)
				delete(universe, ip)
			}

			logger.Info("My universe", "total nodes", len(universe), "nodes", universe)
		}
	}
}

func RegisterNode(etcd *etcdclient.Client, ownIp string, node *Node) {
	key := etcdKeyPrefix + ownIp
	_, err := etcd.Put(context.Background(), key, string(node.Role))
	if err != nil {
		logger.Error("Failed to register node", "err", err)
	}
}

func GetPeerNodes(etcd *etcdclient.Client) {
	// Build universe
	res, err := etcd.Get(context.Background(), etcdKeyPrefix, etcdclient.WithPrefix())
	if err != nil {
		logger.Error("Failed to get etcd data", "err", err)
	}
	for _, proxy := range res.Kvs {
		logger.Info("Found peer proxy", "ip", proxy.Key, "role", proxy.Value)
		ip := string(proxy.Key)[7:]
		universe[ip] = Node{Role: NodeRole(string(proxy.Value)), IP: ip}
	}
	logger.Info("My universe", "total nodes", len(universe), "nodes", universe)
}

func ElectLeader(cli *etcdclient.Client, myIp string) error {
	logger.Info("Starting leader election", "ip", myIp)
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %v", err)
	}
	defer session.Close()

	election := concurrency.NewElection(session, "/proxy-leader")

	// Campaign to be the leader
	ctx := context.TODO()
	if err := election.Campaign(ctx, myIp); err != nil {
		return fmt.Errorf("failed to campaign for leadership: %v", err)
	}

	// We are the leader
	logger.Info("I am the leader", "ip", myIp)

	// Continue to renew the leadership lease
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range time.Tick(5 * time.Second) {
			<-ticker.C
			if err := election.Proclaim(ctx, myIp); err != nil {
				logger.Error("Failed to renew leadership", "err", err)
			}
		}
	}()

	// Listen for leadership changes
	for {
		resp, err := election.Leader(ctx)
		if err != nil {
			return fmt.Errorf("failed to get leader: %v", err)
		}
		leaderIp := string(resp.Kvs[0].Value)
		logger.Info("Current leader", "node", leaderNode)
		leaderNode = universe[leaderIp]

		time.Sleep(5 * time.Second)
	}
}

func ObserveLeader(cli *etcdclient.Client) {

}
