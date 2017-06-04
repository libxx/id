package server

import (
	"log"
	"net"
	"strings"
	"time"

	"github.com/libxx/id/generator"
	"strconv"
)

type DefaultServer struct {
	engine   generator.Engine
	listener net.Listener
}

func NewServer(engine generator.Engine) *DefaultServer {
	s := new(DefaultServer)
	s.engine = engine
	return s
}

func (s *DefaultServer) Serve() error {
	var err error
	s.listener, err = net.Listen("tcp", ":8088")
	if err != nil {
		return err
	}
	log.Printf("listen on :8088")
	for {
		var conn net.Conn
		conn, err = s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				time.Sleep(time.Second)
				log.Printf("temporary error: %s.", err)
				continue
			}
			return err
		}
		log.Println("accept new connection.")
		go s.handleRequest(conn)
	}
}

func (s *DefaultServer) handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		err := conn.SetDeadline(time.Now().Add(time.Second * 10))
		if err != nil {
			return
		}
		req, err := readRequest(conn)
		if err != nil {
			log.Printf("fail to read request: %s.", err)
			if err == errFormat {
				continue
			}
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue
			}
			return
		}

		if len(req) == 0 {
			log.Println("req length is 0.")
			return
		}
		var reply []byte
		switch strings.ToUpper(string(req[0])) {
		case "INCR":
			id, err := s.engine.Next()
			if err != nil {
				reply = NewErrorReply(err.Error())
			} else {
				reply = NewIntegerReply(id)
			}
		case "INCRBY":
			if len(req) != 2 {
				reply = NewErrorReply("invalid arguments")
			} else {
				n, err := strconv.ParseInt(string(req[1]), 10, 64)
				if err != nil {
					reply = NewErrorReply(err.Error())
				} else {
					ids, err := s.engine.NextN(n)
					if err != nil {
						reply = NewErrorReply(err.Error())
					} else {
						reply = NewIntegerReply(ids[len(ids)-1])
					}
				}
			}
		case "GET":
			id, err := s.engine.Current()
			if err != nil {
				reply = NewErrorReply(err.Error())
			} else {
				reply = NewIntegerReply(id)
			}
		default:
			reply = NewErrorReply("unsupported method.")
		}
		_, err = conn.Write(reply)
		if err != nil {
			log.Println(err.Error())
			return
		}
	}
}
