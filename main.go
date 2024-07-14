package main

import (
	"github.com/gookit/goutil/arrutil"
	"log"
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

var (
	mutationCmds = []string{"set", "hset", "del", "hdel", "incr", "decr", "lpush", "rpush", "lpop", "rpop", "lrem", "sadd", "srem", "zadd", "zrem", "zincrby", "hincrby", "hincrbyfloat"}
)

func main() {
	server := Server{role: "primary"}
	err := redcon.ListenAndServe("localhost:7781",
		func(conn redcon.Conn, cmd redcon.Command) {
			log.Printf("Received command: %s", string(cmd.Args[0]))

			// Forward the command to the Redis server
			resp, err := forwardToRedis(cmd)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}

			// Write the response back to the client
			conn.WriteRaw(resp)

			if server.role == "primary" && arrutil.Contains(mutationCmds, strings.ToLower(string(cmd.Args[0]))) {
				log.Printf("Publish command to Kafka")
			}
		},
		func(conn redcon.Conn) bool {
			// This is called when the client connects
			log.Printf("Client connected: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the client disconnects
			log.Printf("Client disconnected: %s", conn.RemoteAddr())
		},
	)

	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
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
