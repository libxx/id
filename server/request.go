package server

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strconv"
)

type Request []string

var errFormat = errors.New("invalid request format")

func readRequest(conn net.Conn) (req Request, err error) {
	reader := bufio.NewReader(conn)
	totalArgLine, err := readLengthLine(reader)
	if err != nil {
		return
	}
	if !bytes.HasPrefix(totalArgLine, []byte{'*'}) {
		err = errFormat
		return
	}
	numArgs, err := strconv.Atoi(string(totalArgLine[1:]))
	if err != nil {
		return
	}

	req = make(Request, 0, numArgs)

	for i := 0; i < numArgs; i++ {
		var argLengthLine []byte
		argLengthLine, err = readLengthLine(reader)
		if err != nil {
			return
		}
		if !bytes.HasPrefix(argLengthLine, []byte{'$'}) {
			err = errFormat
			return
		}
		var argLength int
		argLength, err = strconv.Atoi(string(argLengthLine[1:]))
		if err != nil {
			return
		}

		argLine := make([]byte, argLength+2)
		var n int
		n, err = reader.Read(argLine)
		if err != nil {
			return
		}
		if n != len(argLine) {
			err = errFormat
			return
		}

		if !bytes.HasSuffix(argLine, []byte{'\r', '\n'}) {
			err = errors.New("invalid request")
			return
		}
		arg := argLine[:len(argLine)-2]

		req = append(req, string(arg))
	}

	return
}

func readLengthLine(reader *bufio.Reader) (bs []byte, err error) {
	bs, err = reader.ReadSlice('\r')
	if err != nil {
		return
	}
	next, err := reader.ReadByte()
	if err != nil {
		return
	}
	if next != '\n' {
		err = errFormat
		return
	}
	bs = bs[:len(bs)-1]
	if len(bs) < 2 {
		err = errFormat
	}
	return
}
