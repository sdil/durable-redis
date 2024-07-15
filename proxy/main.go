package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"net"
	"go.etcd.io/etcd/client/v3"

	"github.com/tidwall/redcon"
	"time"
)

var (
	logger *slog.Logger
	node   Node
)

func init() {
	logger = slog.Default()
	node = Node{role: "primary"}
}

func main() {
	// Connect to etcd
	etcdHost := "localhost:2379"
	logger.Info("Connecting to etcd", "host", etcdHost)
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("Failed to connect to etcd", "err", err)
	}
	defer etcd.Close()

	go func() {
		watchEtcd(etcd)
	}()

	// Connect to the real Redis server
	redisHost := "localhost:6379"
	logger.Info("Connecting to redis", "host", redisHost)
	redisConn, err := net.Dial("tcp", redisHost)
	if err != nil {
		logger.Error("Failed to connect to Redis", "err", err)
	}
	defer redisConn.Close()

	// Connect to Kafka
	kafkaHost := "localhost"
	logger.Info("Connecting to Kafka", "host", kafkaHost)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaHost})
	if err != nil {
		logger.Error("Failed to connect to Kafka", "err", err)
	}
	defer producer.Close()

	err = redcon.ListenAndServe("localhost:7781",
		func(conn redcon.Conn, cmd redcon.Command) {
			if node.role == "primary" {
				handleCmdPrimary(conn, cmd, producer, redisConn)
			} else {
				handleCmdReplica(conn, cmd, redisConn)
			}
		},
		func(conn redcon.Conn) bool {
			// This is called when the client connects
			logger.Info("Client connected", "address", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the client disconnects
			logger.Info("Client disconnected", "address", conn.RemoteAddr())
		},
	)

	if err != nil {
		logger.Error("Failed to start server: %v", err)
	}
}
