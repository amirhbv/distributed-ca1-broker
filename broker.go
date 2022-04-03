package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strings"
)

type Message struct {
	msg    string
	sender net.Conn
}

const CHANNEL_LEN = 10

type memoryBroker struct {
	channel  chan *Message
	listener net.Listener
}

func NewMemoryBroker(brokerURI string) (*memoryBroker, error) {
	listener, err := net.Listen("tcp", brokerURI)
	if err != nil {
		return nil, err
	}

	channel := make(chan *Message, CHANNEL_LEN)
	return &memoryBroker{
		channel,
		listener,
	}, nil
}

func (b *memoryBroker) Publish(m string, sender net.Conn) error {
	if len(b.channel) < CHANNEL_LEN {
		m := Message{m, sender}
		b.channel <- &m
		return nil
	} else {
		return fmt.Errorf("Channel Full")
	}
}

func (b *memoryBroker) Subscribe() string {
	m := <-b.channel
	encoder := gob.NewEncoder(m.sender)
	encoder.Encode("ACK")
	return m.msg
}

func (b *memoryBroker) Close() error {
	close(b.channel)
	return nil
}

func (b *memoryBroker) Run() {
	fmt.Println("Started broker on ", b.listener.Addr().String())
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			fmt.Println("Cannot find channel %w", err)
			continue
		}

		fmt.Println("New connection to broker: ", conn.RemoteAddr().String())

		go b.handleConnection(conn)
	}
}

func (b *memoryBroker) handleConnection(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)
	for {

		var receivedMessage string
		decoder.Decode(&receivedMessage)

		if receivedMessage != "" {
			parts := strings.Split(receivedMessage, ":")
			command := parts[0]

			if command == "publish" {
				message := parts[1]
				err := b.Publish(message, conn)
				if err != nil {
					fmt.Println("Error! %v", err)
					encoder.Encode("Error! Channel Full")
				}
			} else if command == "subscribe" {
				message := b.Subscribe()
				encoder.Encode(message)
			}

			fmt.Println("Broker Received a message: ", receivedMessage)
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run broker.go <broker-uri>")
		os.Exit(1)
	}
	brokerURI := os.Args[1]
	broker, err := NewMemoryBroker(brokerURI)
	if err != nil {
		fmt.Println("%v", err)
		os.Exit(1)
	}
	broker.Run()
}
