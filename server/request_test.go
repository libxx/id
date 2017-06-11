package server

import (
	"testing"
	"bufio"
	"strings"
	
	"github.com/stretchr/testify/assert"
)

func TestReadRequest(t *testing.T) {
	assert := assert.New(t)
	reader := bufio.NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n"))
	var (
		req Request
		err error
	)
	req, err = readRequest(reader)
	assert.NoError(err)
	assert.Len(req, 1)
	assert.Contains(req, "PING")

	reader = bufio.NewReader(strings.NewReader("123"))
	req, err = readRequest(reader)
	assert.Equal(errFormat, err)
}
