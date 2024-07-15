package main

import (
	"github.com/tidwall/redcon"
)

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