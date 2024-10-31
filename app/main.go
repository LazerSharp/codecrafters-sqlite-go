package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

type Header struct {
	pageSize uint16
}

func ReadHeader(r io.Reader) (*Header, error) {
	header := make([]byte, 100)
	_, err := r.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	var h Header
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &h.pageSize); err != nil {
		return nil, err
	}

	return &h, nil
}

type PageHeader struct {
	numCell    int16
	pageNumber int32
	pageType   byte
}

func ReadPageHeader(r io.Reader) (*PageHeader, error) {

	// read sql_schema header
	pageHeader := make([]byte, 8)
	_, err := r.Read(pageHeader)
	if err != nil {
		log.Fatal(err)
	}

	var h PageHeader

	if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &h.numCell); err != nil {
		fmt.Println("Failed to read integer:", err)
		return nil, err
	}

	// b-tree page type
	h.pageType = pageHeader[0]

	//fmt.Fprintf(os.Stderr, "Page type: %v\n", h.pageType)
	if h.pageType == 2 || h.pageType == 5 { // interior page
		var pageNumber int32
		if err := binary.Read(r, binary.BigEndian, &h.pageNumber); err != nil {
			fmt.Println("Failed to read integer:", err)
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "Page Number : %v", pageNumber)
	}
	return &h, nil

}

type ColumnSerialType int64

func (st ColumnSerialType) Size() int64 {

	switch {
	case st == 1:
		return 1
	case (st >= 12) && ((st % 2) == 0):
		return int64((st - 12) / 2)
	case (st >= 13) && ((st % 2) == 1):
		return int64((st - 13) / 2)
	}
	return 0
}

func SkipBytes(r io.Reader, n int64) {
	switch rd := r.(type) {
	case io.Seeker:
		rd.Seek(n, io.SeekCurrent)
	default:
		io.CopyN(io.Discard, rd, n)
	}
}

func ReadRecord(r io.Reader) error {
	rcdSize, _, err := ParseVarint(r)
	if err != nil {
		return err
	}
	// skip row_id
	rowId, _, err := ParseVarint(r)

	if err != nil {
		return err
	}

	//fmt.Fprintf(os.Stderr, "record size : %v\n", rcdSize)
	//fmt.Fprintf(os.Stderr, "row id : %v\n", rowId)
	_ = rcdSize
	_ = rowId

	// Record Header
	hdrSize, nbytes, err := ParseVarint(r)
	if err != nil {
		return err
	}
	//fmt.Printf("Size of header %v, bytes in hdrSize %v \n", hdrSize, nbytes)
	// Parse Seial types of columns
	hBytesLeft := hdrSize - int64(nbytes)

	collSerialTypes := make([]ColumnSerialType, 0)

	for {
		if hBytesLeft <= 0 {
			break
		}
		stype, nb, err := ParseVarint(r)
		if err != nil {
			return err
		}
		serialType := ColumnSerialType(stype)
		collSerialTypes = append(collSerialTypes, serialType)
		hBytesLeft -= int64(nb)
		//fmt.Fprintf(os.Stderr, "Serial type : %v \t Size : %v\n", serialType, serialType.Size())
	}

	// read column values - tbl_name for now

	skip := collSerialTypes[0].Size() + collSerialTypes[1].Size()
	SkipBytes(r, skip)

	// read table name
	tblNameBytes := make([]byte, collSerialTypes[2].Size())
	r.Read(tblNameBytes)

	fmt.Printf("%s ", string(tblNameBytes))

	return nil

}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	var h *Header
	if h, err = ReadHeader(databaseFile); err != nil {
		log.Fatal("Failed to read file header", err)
	}

	switch command {

	case ".tables":

		var pHeader *PageHeader
		if pHeader, err = ReadPageHeader(databaseFile); err != nil {
			log.Fatal("Failed to read file header", err)
		}

		// read sql_schema.tbl_name
		// read cell pointer array
		cellPointers := make([]int16, pHeader.numCell)
		if err := binary.Read(databaseFile, binary.BigEndian, &cellPointers); err != nil {
			log.Fatal("error reading cell pointers", err)
		}
		for _, cp := range cellPointers {
			// fmt.Fprintf(os.Stderr, "Cell Pointer #%v : %v\n", i+1, cp)
			databaseFile.Seek(int64(cp), 0)
			ReadRecord(databaseFile)
		}
		fmt.Println()

	case ".dbinfo":
		fmt.Printf("database page size: %v\n", h.pageSize)

		var pHeader *PageHeader
		if pHeader, err = ReadPageHeader(databaseFile); err != nil {
			log.Fatal("Failed to read file header", err)
		}

		fmt.Printf("number of tables: %v\n", pHeader.numCell)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
