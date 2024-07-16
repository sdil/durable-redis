package main

import (
	"context"
	"flag"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.etcd.io/etcd/client/v3"

	"time"

	"github.com/tidwall/redcon"
)

var (
	logger   *slog.Logger
	node     Node
	universe map[string]Node
)

func init() {
	logger = slog.Default()
	universe = map[string]Node{}
}

type Connection struct {
	id      int
	address string
}

func main() {
	var nodeRole = flag.String("role", "primary", "Either primary or replica")
	var port = flag.Int("port", 7781, "Which port to listen on")
	flag.Parse()

	logger.Info("Starting proxy", "role", *nodeRole, "port", *port)
	node = Node{Role: NodeRole(*nodeRole)}

	ownIp := GetLocalIP() + ":" + strconv.Itoa(*port)
	// Initialize the world view
	universe[ownIp] = node

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

	RegisterNode(etcd, ownIp, &node)
	// Build universe
	res, err := etcd.Get(context.Background(), "/proxy/", clientv3.WithPrefix())
	if err != nil {
		logger.Error("Failed to get etcd data", "err", err)
	}
	for _, proxy := range res.Kvs {
		logger.Info("Found proxies", "ip", proxy.Key, "role", proxy.Value)
		ip := string(proxy.Key)[7:]
		universe[ip] = Node{Role: NodeRole(string(proxy.Value))}
	}
	logger.Info("World view", "nodes", universe)

	go func() {
		WatchEtcd(etcd)
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
	logger.Info("Connecting to Kafka to produce messages", "host", kafkaHost)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaHost})
	if err != nil {
		logger.Error("Failed to connect to Kafka", "err", err)
	}
	defer producer.Close()

	logger.Info("Connecting to Kafka to consume messages")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		logger.Error("Failed to consume Kafka messages", "err", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{"redisCmd"}, nil)
	if err != nil {
		logger.Error("Failed to subscribe", "err", err)
	}

	go func() {
		for {
			msg, err := consumer.ReadMessage(time.Second)
			if err == nil {
				handleKafkaMsg(msg, redisConn)
			} else if !err.(kafka.Error).IsTimeout() {
				logger.Info("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}()

	connections := map[redcon.Conn]Connection{}

	go func() {
		err = redcon.ListenAndServe("localhost:"+strconv.Itoa(*port),
			func(conn redcon.Conn, cmd redcon.Command) {
				if node.Role == "primary" {
					handleCmdPrimary(conn, cmd, producer, redisConn)
				} else {
					handleCmdReplica(conn, cmd, redisConn)
				}
				logger.Info("Command responded", "connection", connections[conn])
			},
			func(conn redcon.Conn) bool {
				// This is called when the client connects
				logger.Info("Client connected", "address", conn.RemoteAddr())
				connection := Connection{id: rand.IntN(1000), address: conn.RemoteAddr()}
				connections[conn] = connection
				logger.Info("Client connected", "total", len(connections), "clients", connections)

				// Set a read deadline causes issues with the connection
				// conn.NetConn().SetDeadline(time.Now().Add(10 * time.Second))
				return true
			},
			func(conn redcon.Conn, err error) {
				// This is called when the client disconnects
				logger.Info("Client disconnected", "address", conn.RemoteAddr())
				delete(connections, conn)
				logger.Info("Client connected", "total", len(connections), "clients", connections)
			},
		)

		if err != nil {
			logger.Error("Failed to start server", "error", err)
		}
	}()

	// Capture SIGINT and SIGTERM signals for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	logger.Info("Deleting node", "node", ownIp)
	_, err = etcd.Delete(context.Background(), "/proxy/"+ownIp)
	if err != nil {
		logger.Error("Failed to delete node", "err", err)
	}

	logger.Info("Shutting down gracefully")
}
