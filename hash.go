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

func GetKetamaNumbers(content string) []uint32 {
	digest := GetHashBytes(content)
	numbers := make([]uint32, 0)
	for i := 0; i < KETAMA_NUMBERS_LENGTH; i++ {
		n := ((digest[3+i*4] & 0xFF) << 24) |
			((digest[2+i*4] & 0xFF) << 16) |
			((digest[1+i*4] & 0xFF) << 8) |
			(digest[i*4] & 0xFF)
		numbers = append(numbers, uint32(n))
	}
	return numbers
}

func GetHashForKey(content string) uint32 {
	hashNumbers := GetKetamaNumbers(content)
	var hash uint32
	for _, n := range hashNumbers {
		hash += n
	}
	return hash / uint32(len(hashNumbers))
}
