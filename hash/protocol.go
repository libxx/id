package hash

type Encoder interface {
	Encode(int64) (string, error)
}
