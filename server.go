package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type Server struct {
	b       net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
	mode    string
}

func NewServer(brokerURI string, mode string) (*Server, error) {
	b, err := net.Dial("tcp", brokerURI)
	if err != nil {
		return nil, err
	}
	encoder := gob.NewEncoder(b)
	decoder := gob.NewDecoder(b)
	return &Server{
		b,
		encoder,
		decoder,
		mode,
	}, nil
}

func (s *Server) Send(message string) error {
	err := s.encoder.Encode(strings.Join([]string{"publish", message}, ":"))
	return err
}

func (s *Server) HandleMessage() {
	var result string
	s.decoder.Decode(&result)
	fmt.Printf("received %s result %v\n", s.mode, result)
}

func (s *Server) Run() error {
	i := 1
	for {
		time.Sleep(time.Second * 1)
		msg := fmt.Sprintf("message %d", i)
		i++
		fmt.Println("Sending message: ", msg)
		s.Send(msg)
		if s.mode == "sync" {
			s.HandleMessage()
		} else if s.mode == "async" {
			go s.HandleMessage()
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run server.go <broker-uri> <sync|async>")
		os.Exit(1)
	}
	brokerURI := os.Args[1]
	mode := os.Args[2]
	server, err := NewServer(brokerURI, mode)
	if err != nil {
		fmt.Println("%v", err)
		os.Exit(1)
	}
	err = server.Run()
	if err != nil {
		fmt.Println("%v", err)
		os.Exit(1)
	}
}
