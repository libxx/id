package hash

type BaseEncoder struct {
	chars  []byte
	metric int64
}

func (e BaseEncoder) Encode(id int64) (string, error) {
	encoded := make([]byte, 0, 16)
	for id >= e.metric {
		encoded = append(encoded, e.chars[id%e.metric])
		id = id / e.metric
	}
	encoded = append(encoded, e.chars[id])
	reverseChars(encoded)
	return string(encoded), nil
}

func NewBaseEncoder(charStr string) Encoder {
	encoder := new(BaseEncoder)
	encoder.chars = uniqueChars([]byte(charStr))
	encoder.metric = int64(len(encoder.chars))
	return *encoder
}

func uniqueChars(chars []byte) []byte {
	uniqMap := make(map[byte]struct{}, len(chars))
	uniq := make([]byte, 0, len(chars))
	for _, char := range chars {
		if _, exist := uniqMap[char]; !exist {
			uniqMap[char] = struct{}{}
			uniq = append(uniq, char)
		}
	}
	return uniq[:len(uniqMap)]
}

func reverseChars(chars []byte) {
	length := len(chars)
	bound := length / 2
	for i := 0; i < bound; i++ {
		chars[i], chars[length-i-1] = chars[length-i-1], chars[i]
	}
}
