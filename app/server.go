package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	buff := make([]byte, 50)
	c := bufio.NewReader(conn)

	for {
		// read the full message, or return an error
		n, err := c.Read(buff)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				conn.Close()
				break
			} else {
				fmt.Println("Error reading from connection")
			}
		}

		// split the buffer into lines and print them
		s := string(buff[:n])
		fmt.Println("Received: ", s)

		inputs := strings.Split(s, "\r\n")
		size := len(inputs)

		var positions []int
		for i := 0; i < size; i++ {
			line := inputs[i]
			// if line start with * then it is an array of commands [*2, $4, PING, $4, PONG]
			if strings.HasPrefix(line, "*") {
				j, err := strconv.Atoi(string(line[1]))
				if err != nil {
					fmt.Println("Error reading from connection")
				}
				for k := 0; k < j; k++ {
					positions = append(positions, i+2)
					i += 2
				}
			}
		}
		var commands []string
		for i := 0; i < len(positions); i++ {
			commands = append(commands, inputs[positions[i]])
		}
		fmt.Println(positions, commands)

		response, foundCommand := runCommands(commands)

		fmt.Println("Response: ", foundCommand)
		clearBuffer(buff)

		conn.Write([]byte(response))
	}
}

func clearBuffer(buff []byte) {
	for i := range buff {
		buff[i] = 0
	}
}

func runCommands(commands []string) (string, bool) {
	if len(commands) == 0 {
		return "-ERR Parsing error", false
	}
	entryCommand := strings.ToUpper(commands[0])
	args := commands[1:]
	switch entryCommand {
	case "PING":
		return "+PONG\r\n", true
	case "ECHO":
		if len(args) > 1 {
			return "-ERR wrong number of arguments for 'ECHO' command\r\n", false
		}
		return "+" + args[0] + "\r\n", true
	default:
		return "-ERR unknown command '" + entryCommand + "'" + "\r\n", false
	}
}
