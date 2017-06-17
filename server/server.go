package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/libxx/id/generator"
	"github.com/libxx/id/logging"
	"bufio"
)

type DefaultServer struct {
	engine   generator.Generator
	listener net.Listener
	logFunc  logging.LogFunc
	timeout  time.Duration
}

func NewServer(engine generator.Generator, timeout time.Duration, logFunc logging.LogFunc) *DefaultServer {
	s := new(DefaultServer)
	s.engine = engine
	s.timeout = timeout
	s.logFunc = logging.NewWrapperLogFunc(logFunc)
	return s
}

func (s *DefaultServer) Serve() error {
	var err error
	s.listener, err = net.Listen("tcp", ":8088")
	if err != nil {
		return err
	}
	s.logFunc("listen on :8088")
	for {
		var conn net.Conn
		conn, err = s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				time.Sleep(time.Second)
				s.logFunc(fmt.Sprintf("temporary error: %s.", err))
				continue
			}
			return err
		}
		s.logFunc("accept new connection.")
		go s.handleRequest(conn)
	}
}

func (s *DefaultServer) handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		err := conn.SetDeadline(time.Now().Add(s.timeout))
		if err != nil {
			return
		}
		req, err := readRequest(bufio.NewReader(conn))
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue
			}
			s.logFunc(fmt.Sprintf("fail to read request: %s.", err))
			if err == errFormat {
				continue
			}
			return
		}

		if len(req) == 0 {
			s.logFunc("req length is 0.")
			return
		}
		var reply []byte
		switch strings.ToUpper(string(req[0])) {
		case "PING":
			reply = NewStringReply("PONG")
		case "INCR":
			if len(req) != 2 {
				reply = NewErrorReply("invalid arguments")
			} else {
				id, err := s.engine.Next(string(req[1]))
				if err != nil {
					reply = NewErrorReply(err.Error())
				} else {
					reply = NewIntegerReply(id)
				}
			}
		case "GET":
			if len(req) != 2 {
				reply = NewErrorReply("invalid arguments")
			} else {
				id, err := s.engine.Current(string(req[1]))
				if err != nil {
					reply = NewErrorReply(err.Error())
				} else {
					reply = NewStringReply(strconv.FormatInt(id, 10))
				}
			}
		default:
			reply = NewErrorReply("unsupported method.")
		}
		_, err = conn.Write(reply)
		if err != nil {
			s.logFunc(fmt.Sprintf("fail to write reply: %s", err.Error()))
			return
		}
	}
}
