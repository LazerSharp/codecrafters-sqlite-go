package main

import "io"

func ParseVarint(r io.Reader) (ret int64, nbytes int8, err error) {
	b := make([]byte, 1)
	for {
		_, err = r.Read(b)
		if err != nil {
			return 0, nbytes, err
		}
		nbytes++
		ret = ret | (int64(b[0] & 0x7F))
		if b[0]&0x80 == 0 {
			break
		}
		ret = ret << 7
	}
	return ret, nbytes, nil
}
