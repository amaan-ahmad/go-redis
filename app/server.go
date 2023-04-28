package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "127.0.0.1:6379")
	fmt.Println("Listening on port 6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		println("Waiting for message...")
		netData, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		input := strings.TrimSpace(string(netData))
		if strings.TrimSpace(input) == "stop" {
			fmt.Println("Exiting TCP server!")
			return
		}
		fmt.Println("-> ", input)
		if input == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		} else {
			conn.Write([]byte("-ERR unknown command '" + input + "'\r\n"))
		}
	}
}
