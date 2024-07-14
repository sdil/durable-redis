package main

import (
	"bufio"
	"io"
	"log"
	"net"
)

const (
	port = "7781"
)

func main() {
	// Start a TCP server
	address := "localhost:" + port
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()
	log.Println("Proxy listening on " + address)

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	redisConn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Printf("Failed to connect to Redis server: %v", err)
		return
	}
	defer redisConn.Close()

	go func() {
		_, err := io.Copy(redisConn, clientConn)
		if err != nil {
			log.Printf("Error copying data from client to Redis: %v", err)
		}
	}()

	reader := bufio.NewReader(redisConn)
	writer := bufio.NewWriter(clientConn)

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from Redis connection: %v", err)
			}
			return
		}
		log.Printf("Received from server: %s", line)
		_, err = writer.Write(line)
		if err != nil {
			log.Printf("Error writing to client connection: %v", err)
			return
		}
		log.Printf("Received from client: %s", line)
		writer.Flush()
	}
}
