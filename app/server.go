package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// An Item is something we manage in a priority queue.
type Item struct {
	key    string
	expiry int
	index  int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].expiry < pq[j].expiry
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pg *PriorityQueue) update(item *Item, key string, expiry int) {
	item.key = key
	item.expiry = expiry
	heap.Fix(pg, item.index)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "127.0.0.1:6379")
	fmt.Println("Listening on port 6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	hashmap := make(map[string]string)
	expiryData := make(map[string]time.Time)
	pq := make(PriorityQueue, 1)
	go syncHashmap(hashmap, expiryData)
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
		}
		go handleConnection(conn, hashmap, expiryData)
	}
}

func handleConnection(conn net.Conn, hashmap map[string]string, expiryData map[string]time.Time) {
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

		response, foundCommand := runCommands(commands, hashmap, expiryData)

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

func runCommands(commands []string, hashmap map[string]string, expiryData map[string]time.Time) (string, bool) {
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
	case "SET":
		// if args include PX
		if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
			// if the value is an integer
			seconds, err := strconv.Atoi(args[3])
			expiresAtUnix := time.Now().Add(time.Duration(seconds) * time.Millisecond).Unix()
			if err != nil {
				return "-ERR value is not an integer or out of range\r\n", false
			}
			hashmap[args[0]] = args[1]
			expiryData[args[0]] = time.Unix(expiresAtUnix, 0)
			return "+OK\r\n", true
		}

		if len(args) == 2 {
			hashmap[args[0]] = args[1]
			return "+OK\r\n", true
		}

		return "-ERR wrong number of arguments for 'SET' command\r\n", false

	case "GET":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'GET' command\r\n", false
		}
		value, ok := hashmap[args[0]]
		if !ok {
			return "$-1\r\n", true
		}
		expiry, ok := expiryData[args[0]]
		if ok {
			if time.Now().After(expiry) {
				delete(hashmap, args[0])
				delete(expiryData, args[0])
				return "$-1\r\n", true
			}
		}
		return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n", true
	default:
		return "-ERR unknown command '" + entryCommand + "'" + "\r\n", false
	}
}

func syncHashmap(hashmap map[string]string, expiryData map[string]time.Time) {
	f, err := os.Open("data.txt")
	if err != nil {
		fmt.Println("Error opening file", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ":")
		if len(split) != 2 {
			fmt.Println(line, "is not a valid entry")
			continue
		}
		hashmap[split[0]] = split[1]
	}

	ef, err := os.Open("expiry.txt")
	if err != nil {
		fmt.Println("Error opening file", err)
	}
	defer ef.Close()
	expiryScanner := bufio.NewScanner(ef)
	for expiryScanner.Scan() {
		line := expiryScanner.Text()
		split := strings.Split(line, ":")
		if len(split) != 2 {
			fmt.Println(line, "is not a valid entry")
			continue
		}
		expiry, err := strconv.Atoi(split[1])
		if err != nil {
			fmt.Println("Error converting expiry to int", err)
			continue
		}
		expiryData[split[0]] = time.Unix(int64(expiry), 0)
	}

	// write the hashmap to a file every 5 seconds to persist the data
	for range time.Tick(time.Second * 1) {
		go writeToFile(hashmap)
		go writeExpiryToFile(expiryData)
		fmt.Println("Synced data to file")
	}
}

func writeToFile(hashmap map[string]string) {
	f, err := os.OpenFile("data.txt", os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file", err)
	}
	defer f.Close()
	for key, value := range hashmap {
		f.WriteString(key + ":" + value + "\n")
	}
}

func writeExpiryToFile(expiryData map[string]time.Time) {
	ef, err := os.OpenFile("expiry.txt", os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file", err)
	}
	defer ef.Close()
	for key, value := range expiryData {
		ef.WriteString(key + ":" + strconv.Itoa(int(value.Unix())) + "\n")
	}
}
