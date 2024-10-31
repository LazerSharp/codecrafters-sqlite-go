package main

import (
	"bytes"
	"testing"
)

var varintTestDate = []struct {
	bytes []byte
	val   int64
}{
	{[]byte{0x81, 0x47, 0x01}, 199},
	{[]byte{0x21, 0x47, 0x01}, 33},
}

func TestReadVarint(t *testing.T) {
	for _, td := range varintTestDate {
		source := bytes.NewReader(td.bytes)
		n, err := ParseVarint(source)
		if err != nil {
			t.Error("Parse failed:", err.Error())
		}
		expected := td.val
		if n != expected {
			t.Errorf("Actual: %v Expected: %v", n, expected)
		}
	}
}
