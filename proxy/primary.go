package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gookit/goutil/arrutil"
	"strings"
	"github.com/tidwall/redcon"
	"net"
)

func handleCmdPrimary(conn redcon.Conn, cmd redcon.Command, producer *kafka.Producer, redisConn net.Conn) {
	logger.Info("Received command", "cmd", string(cmd.Args[0]))

	// Forward the command to the Redis server
	resp, err := forwardToRedis(redisConn, cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// Write the response back to the client
	conn.WriteRaw(resp)

	if arrutil.Contains(MutationCmds(), strings.ToLower(string(cmd.Args[0]))) {
		logger.Info("Publish command to Kafka", "message", cmd.Raw)
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(cmd.Raw),
		}, nil)
	}
	return
}
