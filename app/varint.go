package main

import (
	"errors"
	"io"
)

func ParseVarint(r io.Reader) (ret int64, nbytes int8, err error) {
	b := make([]byte, 1)
	for {
		_, err = r.Read(b)
		if err != nil {
			return 0, nbytes, err
		}
		//debug(fmt.Sprintf("Varint Raw Byte %b", b[0]))
		nbytes++
		ret = ret | (int64(b[0] & 0x7F))
		if b[0]&0x80 == 0 {
			break
		}
		ret = ret << 7
	}
	return ret, nbytes, nil
}

func IntFromBytes(bytes []byte) (ret int64, err error) {
	l := len(bytes)
	if l == 0 {
		return 0, nil
	}
	if l > 8 {
		return -1, errors.New("More than 8 bytes of data not allowed")
	}
	for _, b := range bytes {
		ret = ret << 8
		ret = ret | int64(b)
	}
	return ret, nil
}

type Q struct {
	data []int
}

func NewQ(ints []int) *Q {
	return &Q{
		data: ints,
	}
}

func (q *Q) IsEmpty() bool {
	return len(q.data) == 0
}

func (q *Q) Top() (int, bool) {
	if q.IsEmpty() {
		return 0, false
	}
	return q.data[0], true
}

func (q *Q) Pop() (int, bool) {
	ret, ok := q.Top()
	if !ok {
		return 0, ok
	}
	q.data = q.data[1:]
	return ret, ok
}
