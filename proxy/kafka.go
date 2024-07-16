package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/tidwall/redcon"
	"net"
)

func handleKafkaMsg(msg *kafka.Message, redisConn net.Conn) {
	if node.Role == "replica" {
		logger.Info("Forwarding message to Redis server", "msg", msg)
		cmd := redcon.Command{Raw: msg.Value}
		resp, err := forwardToRedis(redisConn, cmd)
		if err != nil {
			logger.Error("Failed to forward message to Redis", "err", err, "resp", resp)
		}
		logger.Info("Applied cmd from Kafka msg", "msg", msg)
	} else {
		logger.Debug("Ignoring Kafka message", "msg", msg)
	}
}
