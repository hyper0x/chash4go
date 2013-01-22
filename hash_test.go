package chash4go

import (
	"testing"
)

func TestGetHashBytes(t *testing.T) {
	content := "127.0.0.1:8080"
	bytes := GetHashBytes(content)
	t.Logf("Hash bytes of content '%v': % x\n", content, bytes)
	length := len(bytes)
	t.Logf("Hash bytes length of content '%v': % d\n", content, length)
	expectedLength := 20
	if length != expectedLength {
		t.Errorf("The length of hash bytes of content '%v' should be %v. (but %v) ", content, expectedLength, length)
		t.FailNow()
	}
	expectedBytes := [...]byte{86, 133, 42, 84, 86, 209, 176, 158, 30, 177, 28, 12, 163, 157, 143, 188, 230, 72, 1, 6}
	for i, b := range bytes {
		expectedByte := expectedBytes[i]
		if b != expectedByte {
			t.Errorf("The %vth of hash bytes of content '%v' should be %v. (but %v) ", i, content, expectedByte, b)
			t.FailNow()
		}
	}
}

func TestGetKetamaNumbers(t *testing.T) {
	content := "127.0.0.1:8080"
	ketamaNumbers := GetKetamaNumbers(content)
	t.Logf("Ketama numbers of content '%v': % d\n", content, ketamaNumbers)
	length := len(ketamaNumbers)
	t.Logf("Ketama numbers length of content '%v': % d\n", content, length)
	expectedLength := 4
	if length != expectedLength {
		t.Errorf("The length of ketama numbers of content '%v' should be %v. (but %v) ", content, expectedLength, length)
		t.FailNow()
	}
	expectedKetamaNumbers := [...]uint64{1412072790, 2662388054, 203206942, 3163528611}
	for i, k := range ketamaNumbers {
		expectedKetamaNumber := expectedKetamaNumbers[i]
		if k != expectedKetamaNumber {
			t.Errorf("The %vth of ketama numbers of content '%v' should be %v. (but %v) ", i, content, expectedKetamaNumber, k)
			t.FailNow()
		}
	}
}

func TestGetHashForKey(t *testing.T) {
	key := "abc"
	keyHash := GetHashForKey(key)
	t.Logf("Key Hash of key '%v': %d\n", key, keyHash)
	expectedKeyHash := uint64(1604963272)
	if keyHash != expectedKeyHash {
		t.Errorf("The hash of key '%v' should be %v. (but %v) ", key, expectedKeyHash, keyHash)
	}
}
