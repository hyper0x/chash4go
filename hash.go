package chash4go

import (
	"crypto/sha1"
	"io"
)

const (
	KETAMA_NUMBERS_LENGTH = 4
)

func GetHashBytes(content string) []byte {
	hash := sha1.New()
	io.WriteString(hash, content)
	bytes := hash.Sum(nil)
	return bytes
}

func GetKetamaNumbers(content string) []uint64 {
	bytes := GetHashBytes(content)
	numbers := make([]uint64, len(bytes))
	for i, b := range bytes {
		numbers[i] = uint64(b)
	}
	ketamaNumbers := make([]uint64, 0)
	for i := 0; i < KETAMA_NUMBERS_LENGTH; i++ {
		n := (numbers[3+i*4]&0xFF)<<24 | (numbers[2+i*4]&0xFF)<<16 | (numbers[1+i*4]&0xFF)<<8 | numbers[i*4]&0xFF
		ketamaNumbers = append(ketamaNumbers, n)
	}
	return ketamaNumbers
}

func GetHashForKey(content string) uint64 {
	hashNumbers := GetKetamaNumbers(content)
	var hash uint64
	for _, n := range hashNumbers {
		hash += n
	}
	return hash / uint64(len(hashNumbers))
}
