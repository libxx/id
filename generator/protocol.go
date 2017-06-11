package generator

import (
	"io"
	"errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist")

type Generator interface {
	io.Closer
	EnableKeys([]string) error
	Next(string) (int64, error)
	Current(string) (int64, error)
}
