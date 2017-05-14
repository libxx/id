package generator

import "io"

type Engine interface {
	io.Closer
	Next() (int64, error)
	Current() (int64, error)
}
