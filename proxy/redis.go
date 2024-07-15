package main

import (
	"github.com/tidwall/redcon"
	"net"
)

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
	// Read the reply from the Redis server

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