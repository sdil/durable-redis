package main

import (
	"github.com/gookit/goutil/arrutil"
	"net"
	"strings"

	"github.com/tidwall/redcon"
)

func handleCmdReplica(conn redcon.Conn, cmd redcon.Command, redisConn net.Conn) {
	if arrutil.Contains(mutationCmds(), strings.ToLower(string(cmd.Args[0]))) {
		// TODO: Forward the request to primary proxy so that 
		// all replicas can receive the mutating commands

		logger.Info("Reject command", "message", cmd.Raw)
		conn.WriteRaw([]byte("-ERR This instance is read-only\r\n"))
		return
	}

	// Forward the command to the Redis server
	resp, err := forwardToRedis(redisConn, cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// Write the response back to the client
	conn.WriteRaw(resp)
}
