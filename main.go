package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gookit/goutil/arrutil"
	"log/slog"
	"net"
	"strings"

	"github.com/tidwall/redcon"
)

type Server struct {
	role ServerRole
}

type ServerRole string

const (
	ServerRolePrimary ServerRole = "primary"
	ServerRoleReplica ServerRole = "replica"
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
	server := Server{role: "primary"}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		logger.Error("Failed to connect to Kafka", "err", err)
	}
	defer producer.Close()
	topic = "redisCmd"

	err = redcon.ListenAndServe("localhost:7781",
		func(conn redcon.Conn, cmd redcon.Command) {
			if server.role == "primary" {
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

func handleCmdPrimary(conn redcon.Conn, cmd redcon.Command) {
	logger.Info("Received command", "cmd", string(cmd.Args[0]))

	// Forward the command to the Redis server
	resp, err := forwardToRedis(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// Write the response back to the client
	conn.WriteRaw(resp)

	// if arrutil.Contains(MutationCmds(), strings.ToLower(string(cmd.Args[0]))) {
	// 	logger.Info("Publish command to Kafka", "message", cmd.Raw)
	// 	producer.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(cmd.Raw),
	// 	}, nil)
	// }
	return
}

func handleCmdReplica(conn redcon.Conn, cmd redcon.Command) {
	if arrutil.Contains(MutationCmds(), strings.ToLower(string(cmd.Args[0]))) {
		logger.Info("Reject command", "message", cmd.Raw)
		conn.WriteRaw([]byte("-ERR This instance is read-only\r\n"))
		return
	}

	// Forward the command to the Redis server
	resp, err := forwardToRedis(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// Write the response back to the client
	conn.WriteRaw(resp)
	return
}

func forwardToRedis(cmd redcon.Command) ([]byte, error) {
	// Connect to the real Redis server
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write the command to the Redis server
	_, err = conn.Write(cmd.Raw)
	if err != nil {
		return nil, err
	}

	// Read the response from the Redis server
	resp, err := readAll(conn)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func readAll(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmp[:n]...)
		if n < len(tmp) {
			break
		}
	}
	return buf, nil
}
