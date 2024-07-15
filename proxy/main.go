package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"net"

	"github.com/tidwall/redcon"
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
	// Connect to the real Redis server
	redisConn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		logger.Error("Failed to connect to Redis", "err", err)
	}
	defer redisConn.Close()

	// Connect to Kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
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
