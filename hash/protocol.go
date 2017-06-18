package hash

type Encoder interface {
	Encode(int64) (string, error)
	Decode(string) (int64, error)
}
