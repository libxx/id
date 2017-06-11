package server

import (
	"bufio"
	"errors"
	"strconv"
)

type Request []string

var errFormat = errors.New("invalid request format")

func readRequest(reader *bufio.Reader) (req Request, err error) {
	// parse command arguments count
	numArgs, err := readArgumentNum(reader)
	if err != nil {
		return
	}

	req = make(Request, 0, numArgs)

	for i := 0; i < numArgs; i++ {
		var arg []byte
		arg, err = readArgument(reader)
		if err != nil {
			return
		}

		req = append(req, string(arg))
	}

	return
}

func readArgumentNum(reader *bufio.Reader) (n int, err error) {
	body, err := readProtocolLine('*', reader)
	if err != nil {
		return
	}
	n, err = strconv.Atoi(string(body))
	return
}

func readArgument(reader *bufio.Reader) (arg []byte, err error) {
	body, err := readProtocolLine('$', reader)
	if err != nil {
		return
	}

	argLength, err := strconv.Atoi(string(body))
	if err != nil {
		return
	}

	arg, err = readBytes(argLength, reader)
	return
}

func readProtocolLine(prefix byte, reader *bufio.Reader) (body []byte, err error) {
	p, err := reader.ReadByte()
	if err != nil {
		return
	}
	if p != prefix {
		err = errFormat
		return
	}
	content, err := reader.ReadBytes('\r')
	if err != nil {
		return
	}
	if len(content) == 1 {
		err = errFormat
		return
	}
	lf, err := reader.ReadByte()
	if err != nil {
		return
	}
	if lf != '\n' {
		err = errFormat
		return
	}
	body = make([]byte, len(content)-1)
	copy(body, content)
	return
}

func readBytes(length int, reader *bufio.Reader) (body []byte, err error) {
	body = make([]byte, length)
	i := 0
	for i < length {
		body[i], err = reader.ReadByte()
		if err != nil {
			return
		}
		i++
	}

	crlf := make([]byte, 2)
	n, err := reader.Read(crlf)
	if n != 2 || string(crlf) != "\r\n" {
		err = errFormat
		return
	}
	return
}
