package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	KILOBYTE = 1024
	PACKET_SIZE = 2 * KILOBYTE
	MAX_PACKETS = 2048
	BUFFER_SIZE = PACKET_SIZE * MAX_PACKETS

)

func main() {
	positionals := os.Args[1:]

	if len(positionals) < 3 {
		log.Fatalf("Usage: ./data-reader <ip> <port> <name>")
	}

	ip := net.ParseIP(positionals[0])
	port, err := strconv.ParseInt(positionals[1], 10, 16)
	if err != nil {
		log.Fatalf("Failed to parse port: %v cannot be parsed: %v", positionals[1], err)
	}
	outfile := positionals[2]

	file, err := os.OpenFile(fmt.Sprintf("./%v.f1", outfile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	addr := &net.UDPAddr{
		IP:   ip,
		Port: int(port),
		Zone: "",
	}
	serverAddr, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Error dialing UDP: %v", err)
	}
	defer serverAddr.Close()

	fmt.Println("Successfully dialed UDP addr")

	doneChan := make(chan error, 1)
	cancelChan := make(chan os.Signal, 1)
	buffer := make([]byte, BUFFER_SIZE)
	go func() {
		pktCounter := 0
		for {
			select {
			case <-cancelChan:
				doneChan <- nil
				return
			default:
				pktCounter += 1
				nRead, _, err := serverAddr.ReadFromUDP(buffer)
				if err != nil {
					doneChan <- err
					return
				}

				fmt.Println(fmt.Sprintf("Read packet: %v", pktCounter))
				data := buffer[:nRead]

				_, err = file.WriteString(string(data) + "\n")
				if err != nil {
					doneChan <- err
					return
				}
			}
		}
	}()

	signal.Notify(cancelChan, os.Interrupt, syscall.SIGINT)

	err = <-doneChan
	if err != nil {
		log.Fatalf("Error reading packets: %v", err)
	}
	fmt.Println("Successfully cleanly terminated data reader")
}
