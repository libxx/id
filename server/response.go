package server

import "strconv"

func NewStringReply(reply string) []byte {
	return newSimpleReply('+', reply)
}

func NewErrorReply(reply string) []byte {
	return newSimpleReply('-', reply)
}

func NewIntegerReply(num int64) []byte {
	return newSimpleReply(':', strconv.FormatInt(num, 10))
}

func newSimpleReply(prefix byte, reply string) []byte {
	return append(append([]byte{prefix}, []byte(reply)...), '\r', '\n')
}