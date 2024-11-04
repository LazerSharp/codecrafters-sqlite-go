package main

import (
	"bytes"
	"testing"
)

type ByteToIntTestData []struct {
	bytes []byte
	val   int64
}

var varintTestData = ByteToIntTestData{
	{[]byte{0x81, 0x47, 0x01}, 199},
	{[]byte{0x21, 0x47, 0x01}, 33},
}

func TestReadVarint(t *testing.T) {
	for _, td := range varintTestData {
		source := bytes.NewReader(td.bytes)
		n, _, err := ParseVarint(source)
		if err != nil {
			t.Error("Parse failed:", err.Error())
		}
		expected := td.val
		if n != expected {
			t.Errorf("Actual: %v Expected: %v", n, expected)
		}
	}
}

var intFromBytesTestData = ByteToIntTestData{
	{[]byte{0x11}, 17},
}

func TestIntFromBytes(t *testing.T) {
	for _, td := range intFromBytesTestData {
		i, err := IntFromBytes(td.bytes)
		if err != nil {
			t.Error("Parse failed: ", err.Error())
		}
		expected := td.val
		if i != expected {
			t.Errorf("Actual: %v Expected: %v", i, expected)
		}
	}
}
