package chash4go

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	content := "abcdefghijklmnopqrstuvwxyz"
	bytes := GetHashBytes(content)
	fmt.Printf("Hash Bytes: % x\n", bytes)
	length := len(bytes)
	fmt.Printf("Hash Bytes Len: % d\n", length)
	expectedLength := 20
	if length != expectedLength {
		t.Errorf("'%d' is not equals '%d'", length, expectedLength)
		t.FailNow()
	}
	numbers := GetKetamaNumbers(content)
	fmt.Printf("Ketama numbers: % d\n", numbers)
	hash := GetHashForKey(content)
	fmt.Printf("Hash: %d\n", hash)
}
