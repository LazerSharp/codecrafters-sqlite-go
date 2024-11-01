package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	// Available if you need it!
	"github.com/xwb1989/sqlparser"
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

func ReadPageHeader(r io.ReadSeeker, page int) (*PageHeader, error) {
	offset := 0
	if page == 1 {
		offset = 100
	}
	JumpToPage(r, h.pageSize, page, offset)
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
	StringVal
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
		return StringVal
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
	case StringVal:
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
	if ctype != StringVal {
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

func ReadRecords(r io.ReadSeeker, numCells int16, rootPage int) (*[]Record, error) {
	// read cell pointer array
	cellPointers := make([]int16, numCells)
	recoreds := make([]Record, 0, numCells)
	if err := binary.Read(r, binary.BigEndian, &cellPointers); err != nil {
		fmt.Fprintln(os.Stderr, "error reading cell pointers", err)
		return nil, err
	}
	for _, cp := range cellPointers {
		JumpToPage(r, h.pageSize, rootPage, int(cp))
		record, err := ReadRecord(r)
		if err != nil {
			return nil, err
		}
		recoreds = append(recoreds, *record)
	}
	return &recoreds, nil
}

func ReadPage(r io.ReadSeeker, rootPage int) (*[]Record, error) {
	pHeader, err := ReadPageHeader(r, rootPage)
	if err != nil {
		log.Println("Failed to read file header: ", err)
		return nil, err
	}
	records, err := ReadRecords(r, pHeader.numCell, rootPage)
	if err != nil {
		log.Println("Failed to read records: ", err)
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
	records, _ := ReadPage(r, 1)
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

func parseDDL(ddlSql string) *sqlparser.DDL {

	sql := strings.ReplaceAll(ddlSql, "autoincrement", "")
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		log.Fatal("Error parsing SQL")
	}

	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok {
		log.Fatal("Not a DDL statement")
	}

	return ddl

}

func parseSelectQuery(selectSql string) *sqlparser.Select {
	stmt, err := sqlparser.Parse(selectSql)
	if err != nil {
		log.Fatal("Error parsing SQL")
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		log.Fatal("Not a valid select stmt.")
	}
	return selectStmt
}

func fetchTableMetadata(r io.ReadSeeker, tableName string) (*SqlSchemaRecord, error) {
	recs, err := ReadSqlSchema(r)
	if err != nil {
		return nil, err
	}
	for _, rec := range *recs {
		if rec.TablName == tableName {
			return &rec, nil
		}
	}
	return nil, errors.New("Table not found")
}

func JumpToPage(seeker io.Seeker, pageSize uint16, page int, offset int) {
	// jump to rootpage
	_, err := seeker.Seek(int64(pageSize*uint16(page-1))+int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
}

var h *Header

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

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
		if pHeader, err = ReadPageHeader(databaseFile, 1); err != nil {
			log.Fatal("Failed to read file header", err)
		}

		fmt.Printf("number of tables: %v\n", pHeader.numCell)

	default:

		// read select Query

		selectQuery := command

		selectStmt := parseSelectQuery(selectQuery)
		tblName := sqlparser.String(selectStmt.From)
		selectColumn := sqlparser.String(selectStmt.SelectExprs)

		selectColumns := map[string]bool{}
		for _, expr := range selectStmt.SelectExprs {
			selectColumns[sqlparser.String(expr)] = true
		}

		tblMetaData, err := fetchTableMetadata(databaseFile, tblName)

		if err != nil {
			log.Fatal("Error fetching meta data", err)
		}

		switch {
		case strings.ToUpper(selectColumn) == "COUNT(*)":

			rootPage := tblMetaData.RootPage
			tblHeader, err := ReadPageHeader(databaseFile, rootPage)
			if err != nil {
				log.Fatal("Uanble to read Page Header: ", err)
			}
			fmt.Println(tblHeader.numCell)

		default:

			ddlSql := tblMetaData.Sql
			ddlStmt := parseDDL(ddlSql)

			colIndices := make([]int, 0, len(ddlStmt.TableSpec.Columns))
			for i, col := range ddlStmt.TableSpec.Columns {
				colName := col.Name.String()
				if selectColumns[colName] {
					colIndices = append(colIndices, i)
				}

			}
			rootPage := tblMetaData.RootPage
			recs, err := ReadPage(databaseFile, rootPage)
			if err != nil {
				log.Fatal("Error reading page", err)
			}
			for _, rec := range *recs {
				colVals := make([]string, 0, len(colIndices))
				for _, colIndex := range colIndices {
					colVals = append(colVals, rec.columns[colIndex].StringVal())
				}
				fmt.Println(strings.Join(colVals, "|"))
			}
		}
	}
}
