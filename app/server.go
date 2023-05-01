package main

import (
	"bufio"
	"fmt"
	"io"
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
	defer l.Close()
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection")
		os.Exit(1)
	}
	buff := make([]byte, 50)
	c := bufio.NewReader(conn)

	for {
		foundCommand := false
		// read the full message, or return an error
		n, err := c.Read(buff)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				os.Exit(0)
			}
			fmt.Println("Error reading from connection")
			os.Exit(1)
		}

		// split the buffer into lines and print them
		s := string(buff[:n])
		fmt.Println("Received: ", s)
		for _, line := range strings.Split(s, "\r\n") {
			line = strings.ToUpper(strings.TrimSpace(line))
			if line == "PING" {
				foundCommand = true
				clearBuffer(buff)
				conn.Write([]byte("+PONG\r\n"))
				break
			}
		}
		if foundCommand {
			continue
		}
		clearBuffer(buff)
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func clearBuffer(buff []byte) {
	for i := range buff {
		buff[i] = 0
	}
}
