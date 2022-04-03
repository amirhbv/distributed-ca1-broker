package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

type Client struct {
	sock net.Conn
}

func NewClient(brokerURI string) (*Client, error) {
	sock, err := net.Dial("tcp", brokerURI)
	if err != nil {
		return nil, err
	}
	return &Client{
		sock,
	}, nil
}

func (c *Client) Start() error {
	encoder := gob.NewEncoder(c.sock)
	decoder := gob.NewDecoder(c.sock)
	for {
		err := encoder.Encode("subscribe")
		if err != nil {
			return err
		}
		var receivedMessage string
		decoder.Decode(&receivedMessage)
		fmt.Println("Client received a message: ", receivedMessage)
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run client.go <broker-uri>")
		os.Exit(1)
	}
	brokerURI := os.Args[1]
	client, err := NewClient(brokerURI)
	if err != nil {
		fmt.Println("%v", err)
		os.Exit(1)
	}
	client.Start()
}
