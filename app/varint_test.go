package main

import (
	"bytes"
	"fmt"
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

func TestQ(t *testing.T) {

	q := NewQ([]int{1, 2, 3})

	fmt.Println(q.data)

	if x, _ := q.Top(); x != 1 {
		t.Error("Top should be 1 found: ", x)
	}

	if x, ok := q.Pop(); x != 1 || !ok {
		t.Error("Pop should return 1")
	}

	if x, ok := q.Top(); x != 2 || !ok {
		t.Error("Top should be 2")
	}

	q.Pop()
	q.Pop()

	if !q.IsEmpty() {
		t.Error("'q' should be emplty!")
	}

}
