package main

import (
	"net"
)

func GetLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logger.Error(err.Error())
	}
	defer conn.Close()

	localAddress := conn.LocalAddr().(*net.UDPAddr)
	logger.Info("Local IP address", "ip", localAddress.IP.String())

	return localAddress.IP.String()
}
