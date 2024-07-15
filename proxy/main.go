package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"

	"github.com/tidwall/redcon"
)


func MutationCmds() []string {
	return []string{"set", "hset", "del", "hdel", "incr", "decr", "lpush", "rpush", "lpop", "rpop", "lrem", "sadd", "srem", "zadd", "zrem", "zincrby", "hincrby", "hincrbyfloat"}
}

var (
	logger   *slog.Logger
	topic    string
	producer *kafka.Producer
)

func main() {
	logger = slog.Default()
	node := Node{role: "primary"}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		logger.Error("Failed to connect to Kafka", "err", err)
	}
	defer producer.Close()
	topic = "redisCmd"

	err = redcon.ListenAndServe("localhost:7781",
		func(conn redcon.Conn, cmd redcon.Command) {
			if node.role == "primary" {
				handleCmdPrimary(conn, cmd)
			} else {
				handleCmdReplica(conn, cmd)
			}
		},
		func(conn redcon.Conn) bool {
			// This is called when the client connects
			logger.Info("Client connected", "client_address", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the client disconnects
			logger.Info("Client disconnected", "client_address", conn.RemoteAddr())
		},
	)

	if err != nil {
		logger.Info("Failed to start server: %v", err)
	}
}
