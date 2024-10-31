package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
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

type ColumnType int

const (
	Null ColumnType = iota // Value is a NULL.
	Int8
	Int16
	Int24
	Int32
	Int48
	Int64
	Float
	ZeroInt
	OneInt
	_ // Reserved
	_ // Reserved
	Blob
	String
)

func (t ColumnSerialType) Type() ColumnType {

	switch {
	case t == 0:
		return Null
	case t == 1:
		return Int8
	//Todo: add more cases
	case (t >= 12) && ((t % 2) == 0):
		return Blob
	case (t >= 13) && ((t % 2) == 1):
		return String
	default:
		return Null
	}
}

func (st ColumnSerialType) Size() int64 {

	switch st.Type() {
	case Int8:
		return 1
	case Blob:
		return int64((st - 12) / 2)
	case String:
		return int64((st - 13) / 2)
	default:
		return 0
	}
}

func SkipBytes(r io.Reader, n int64) {
	switch rd := r.(type) {
	case io.Seeker:
		rd.Seek(n, io.SeekCurrent)
	default:
		io.CopyN(io.Discard, rd, n)
	}
}

type Column struct {
	serialType *ColumnSerialType
	bytes      []byte
}

func ReadColumn(ctyp *ColumnSerialType, r io.Reader) (*Column, error) {
	data := make([]byte, ctyp.Size())
	_, err := r.Read(data)
	if err != nil {
		return nil, err
	}
	return &Column{
		serialType: ctyp,
		bytes:      data,
	}, nil
}

func (c Column) StringVal() string {
	ctype := c.serialType.Type()
	if ctype != String {
		log.Fatal("Column type is not string. It is %v instread.", ctype)
	}
	return string(c.bytes)
}

func (c Column) Int8Val() int {
	ctype := c.serialType.Type()
	if ctype != Int8 {
		log.Fatal("Column type is not string. It is %v instread.", ctype)
	}
	return int(c.bytes[0])
}

type Record struct {
	columns []Column
}

func ReadRecord(r io.Reader) (*Record, error) {
	rcdSize, _, err := ParseVarint(r)
	if err != nil {
		return nil, err
	}
	// skip row_id
	rowId, _, err := ParseVarint(r)

	if err != nil {
		return nil, err
	}

	//fmt.Fprintf(os.Stderr, "record size : %v\n", rcdSize)
	//fmt.Fprintf(os.Stderr, "row id : %v\n", rowId)
	_ = rcdSize
	_ = rowId

	// Record Header
	hdrSize, nbytes, err := ParseVarint(r)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		serialType := ColumnSerialType(stype)
		collSerialTypes = append(collSerialTypes, serialType)
		hBytesLeft -= int64(nb)
		//fmt.Fprintf(os.Stderr, "Serial type : %v \t Size : %v\n", serialType, serialType.Size())
	}

	// read column values

	columns := make([]Column, 0, len(collSerialTypes))

	for _, collSerialType := range collSerialTypes {
		column, err := ReadColumn(&collSerialType, r)
		if err != nil {
			return nil, err
		}
		columns = append(columns, *column)
	}

	return &Record{
		columns: columns,
	}, nil

}

func ReadRecords(r io.ReadSeeker, numCells int16) (*[]Record, error) {
	// read cell pointer array
	cellPointers := make([]int16, numCells)
	recoreds := make([]Record, 0, numCells)
	if err := binary.Read(r, binary.BigEndian, &cellPointers); err != nil {
		fmt.Fprintln(os.Stderr, "error reading cell pointers", err)
		return nil, err
	}

	for _, cp := range cellPointers {
		// fmt.Fprintf(os.Stderr, "Cell Pointer #%v : %v\n", i+1, cp)
		r.Seek(int64(cp), 0)
		record, err := ReadRecord(r)
		if err != nil {
			return nil, err
		}
		recoreds = append(recoreds, *record)
	}

	return &recoreds, nil

}

func ReadPage(r io.ReadSeeker) (*[]Record, error) {

	pHeader, err := ReadPageHeader(r)
	if err != nil {
		log.Println("Failed to read file header", err)
		return nil, err
	}
	records, err := ReadRecords(r, pHeader.numCell)
	if err != nil {
		log.Println("Failed to read records", err)
		return nil, err
	}
	return records, nil
}

type SqlSchemaRecord struct {
	Typ      string
	Name     string
	TablName string
	RootPage int
	Sql      string
}

func ReadSqlSchema(r io.ReadSeeker) (*[]SqlSchemaRecord, error) {
	// Assuming the head on page 1 and file header (initial 100 bytes) already consumed / skipped
	records, _ := ReadPage(r)
	schemaRecods := make([]SqlSchemaRecord, 0, len(*records))

	for _, rec := range *records {
		schemaRecord := SqlSchemaRecord{
			Typ:      rec.columns[0].StringVal(),
			Name:     rec.columns[1].StringVal(),
			TablName: rec.columns[2].StringVal(),
			RootPage: rec.columns[3].Int8Val(),
			Sql:      rec.columns[4].StringVal(),
		}
		schemaRecods = append(schemaRecods, schemaRecord)
	}
	return &schemaRecods, nil

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
		recods, err := ReadSqlSchema(databaseFile)
		if err != nil {
			log.Fatal(err)
		}
		for _, rec := range *recods {
			fmt.Printf("%v ", rec.TablName)
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
		arr := strings.Split(command, " ")
		tblName := arr[len(arr)-1]
		//fmt.Printf("Table name: [%v]\n", tblName)

		recods, err := ReadSqlSchema(databaseFile)
		if err != nil {
			log.Fatal(err)
		}
		var rootPage int
		for _, rec := range *recods {
			if rec.TablName == tblName {
				rootPage = rec.RootPage
			}
		}
		//fmt.Printf("Root Page #: [%v]\n", rootPage)

		// jump to rootpage
		_, err = databaseFile.Seek(int64(h.pageSize*uint16(rootPage-1)), io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}

		tblHeader, err := ReadPageHeader(databaseFile)
		fmt.Println(tblHeader.numCell)

	}
}
