package main

import (
	"net"

	"github.com/tidwall/redcon"
)

func mutationCmds() []string {
	return []string{"set", "hset", "del", "hdel", "incr", "decr", "lpush", "rpush", "lpop", "rpop", "lrem", "sadd", "srem", "zadd", "zrem", "zincrby", "hincrby", "hincrbyfloat"}
}

func forwardToRedis(conn net.Conn, cmd redcon.Command) ([]byte, error) {
	// Write the command to the Redis server
	logger.Info("Forwading redis cmd", "raw", string(cmd.Raw))
	_, err := conn.Write(cmd.Raw)
	if err != nil {
		return nil, err
	}

	// Read the response from the Redis server
	resp, err := readAll(conn)
	if err != nil {
		logger.Info("Redis error", "error", string(err.Error()))
		return nil, err
	}

	if len(string(resp)) < 1000 {
		logger.Info("Redis response", "resp", string(resp))
	}

	return resp, nil
}

func readAll(conn net.Conn) ([]byte, error) {
	// Read the reply from the Redis server
	// This is from ChatGPT, don't ask me how

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
