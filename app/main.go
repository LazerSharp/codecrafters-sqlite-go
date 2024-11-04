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

type PageType byte

// page types
const (
	TableLeafPage     PageType = 0x0d // 13
	TableInteriorPage PageType = 0x05 // 5
	IndexLeafPage     PageType = 0x0a // 10
	IndexInteriorPage PageType = 0x02 // 2
)

type PageHeader struct {
	numCell      uint16
	rightPageNum uint32 // pointer
	pageType     PageType
}

func ReadPageHeader(r io.ReadSeeker, page uint32) (*PageHeader, error) {
	offset := uint16(0)
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
	h.pageType = PageType(pageHeader[0])

	//fmt.Fprintf(os.Stderr, "Page type: %v\n", h.pageType)
	if h.pageType == TableInteriorPage || h.pageType == IndexInteriorPage { // interior page
		if err := binary.Read(r, binary.BigEndian, &h.rightPageNum); err != nil {
			fmt.Println("Failed to read integer:", err)
			return nil, err
		}
		debug(fmt.Sprintf("Right Page Number : [%v] \n", h.rightPageNum))
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
	case t == 2:
		return Int16
	case t == 3:
		return Int24
	case t == 4:
		return Int32
	case t == 5:
		return Int48
	case t == 6:
		return Int64
	case t == 7:
		return Float
	case t == 8:
		return ZeroInt
	case t == 9:
		return OneInt
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
	case Int16:
		return 2
	case Int24:
		return 3
	case Int32:
		return 4
	case Int48:
		return 6
	case Int64:
		return 8
	case Float:
		return 8
	case ZeroInt, OneInt:
		return 0
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

	//fmt.Printf("c.serialType: %v\n", c.serialType.Type())
	ctype := c.serialType.Type()
	if ctype == Null {
		return ""
	}
	if ctype != StringVal {
		log.Fatalf("Column type is not string. It is %v instead.", ctype)
	}
	return string(c.bytes)
}

func (c Column) IntVal() int64 {
	ctype := c.serialType.Type()
	if !(ctype == Int8 || ctype == Int16 || ctype == Int24 || ctype == Int32) {
		log.Fatalf("Column type is not Int8 / Int 16 / Int32. It is %v instead.", ctype)
	}
	n, err := IntFromBytes(c.bytes)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

type Record struct {
	columns []Column
	rowId   int64
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

	columns, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	return &Record{
		columns: *columns,
		rowId:   rowId,
	}, nil

}

func readPayload(r io.Reader) (*[]Column, error) {
	// Record Header
	hdrSize, nbytes, err := ParseVarint(r)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Size of header %v, bytes in hdrSize %v \n", hdrSize, nbytes)
	// Parse Seial types of columns
	hBytesLeft := hdrSize - int64(nbytes)
	debug(fmt.Sprintf("Bytes for columns: %d", hBytesLeft))

	collSerialTypes := make([]ColumnSerialType, 0)

	for {
		if hBytesLeft <= 0 {
			break
		}
		debug("Parsing Serial type ...")
		stype, nb, err := ParseVarint(r)
		if err != nil {
			return nil, err
		}
		debug(fmt.Sprintf("Serial type [%d] nbytes = [%d]", stype, nbytes))
		serialType := ColumnSerialType(stype)
		collSerialTypes = append(collSerialTypes, serialType)
		hBytesLeft -= int64(nb)
		//fmt.Fprintf(os.Stderr, "Serial type : %v \t Size : %v\n", serialType, serialType.Size())
	}

	// read column values

	columns := make([]Column, 0, len(collSerialTypes))

	debug(fmt.Sprintf("No of columns: %d", len(collSerialTypes)))

	for _, collSerialType := range collSerialTypes {
		column, err := ReadColumn(&collSerialType, r)
		if err != nil {
			return nil, err
		}
		columns = append(columns, *column)
	}
	return &columns, nil
}

type IndexRecord struct {
	leftPageNum uint32
	rowId       int64
	key         string
}

func ReadIndexRecord(r io.ReadSeeker, isInterior bool) (*IndexRecord, error) {
	var leftPageNum uint32
	if isInterior {
		err := binary.Read(r, binary.BigEndian, &leftPageNum)
		if err != nil {
			return nil, err
		}
	}
	rcdSize, _, err := ParseVarint(r)
	if err != nil {
		return nil, err
	}
	_ = rcdSize // ignore record size
	cols, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	ir := IndexRecord{
		leftPageNum: leftPageNum,
		key:         (*cols)[0].StringVal(),
		rowId:       (*cols)[1].IntVal(),
	}
	debug(fmt.Sprintf("[%s] => #%d", ir.key, ir.rowId))
	return &ir, nil
}

func ReadLeafPageRecords(r io.ReadSeeker, numCells uint16, rootPage uint32, rowIds *Q) (*[]Record, error) {
	// read cell pointer array
	cellPointers := make([]uint16, numCells)
	recoreds := make([]Record, 0, numCells)
	if err := binary.Read(r, binary.BigEndian, &cellPointers); err != nil {
		fmt.Fprintln(os.Stderr, "error reading cell pointers", err)
		return nil, err
	}
	for _, cp := range cellPointers {
		JumpToPage(r, h.pageSize, rootPage, cp)
		record, err := ReadRecord(r)
		if err != nil {
			return nil, err
		}
		if rowIds != nil {
			if rowIds.IsEmpty() { // no more row ids to be searched
				break
			}
			rowId, _ := rowIds.Top()
			if record.rowId != int64(rowId) { // row id does not match - Skip!
				continue
			} else {
				rowIds.Pop()
			}
		}
		recoreds = append(recoreds, *record)
	}
	return &recoreds, nil
}

func ReadTablePage(r io.ReadSeeker, pageNum uint32, rowIds *Q) (*[]Record, error) {

	debug(fmt.Sprintf("Reading Page Number: <%d>\n", pageNum))

	pHeader, err := ReadPageHeader(r, pageNum)
	if err != nil {
		log.Println("Failed to read file header: ", err)
		return nil, err
	}

	// handle leaf page - Read all the records
	if pHeader.pageType == TableLeafPage {
		debug("<<<<<Leaf Page>>>>")
		records, err := ReadLeafPageRecords(r, pHeader.numCell, pageNum, rowIds)
		if err != nil {
			log.Println("Failed to read records: ", err)
			return nil, err
		}
		return records, nil
	}

	if pHeader.pageType == TableInteriorPage {

		debug(fmt.Sprintf("<<<<<Interior Page>>>> cells [%d]", pHeader.numCell))
		records, err := TraverseInteriorPage(r, pHeader.numCell, pageNum, pHeader.rightPageNum, rowIds)
		if err != nil {
			log.Println("Failed to read records: ", err)
			return nil, err
		}
		return records, nil
	}
	return nil, fmt.Errorf("Read Page: Invalid page type : [%d]", pHeader.pageType)
}

func ReadIndexPage(r io.ReadSeeker, pageNum uint32, key string) (*[]int, error) {

	debug(fmt.Sprintf("Reading Page Number: <%d>\n", pageNum))

	pHeader, err := ReadPageHeader(r, pageNum)
	if err != nil {
		log.Println("Failed to read file header: ", err)
		return nil, err
	}

	if pHeader.pageType == IndexLeafPage {
		debug(fmt.Sprintf("Page# %d is an index-leaf page", pageNum))
		return ReadIndexLeafPage(r, pHeader.numCell, pageNum, key)
	}

	if pHeader.pageType == IndexInteriorPage {
		debug(fmt.Sprintf("Page# %d is an index-interior page", pageNum))
		return TraverseIndexInteriorPage(r, pHeader.numCell, pageNum, pHeader.rightPageNum, key)
	}
	return nil, fmt.Errorf("Read Index Page: Invalid page type : [%d]", pHeader.pageType)
}

func ReadIndexPageBody(r io.ReadSeeker, numCells uint16, pageNum uint32, key string, rightPageNum uint32, isInterior bool) (*[]int, error) {

	// read cell pointer array
	cellPointers := make([]uint16, numCells)
	//recoreds := make([]IndexRecord, 0, numCells)
	if err := binary.Read(r, binary.BigEndian, &cellPointers); err != nil {
		fmt.Fprintln(os.Stderr, "error reading cell pointers", err)
		return nil, err
	}
	debug(fmt.Sprintf("# of cells: %d", numCells))

	rowIds := make([]int, 0)
	for _, cp := range cellPointers {
		JumpToPage(r, h.pageSize, pageNum, cp)
		record, err := ReadIndexRecord(r, isInterior)
		if err != nil {
			return nil, err
		}
		cmp := strings.Compare(key, record.key)

		if cmp == 0 {
			rowIds = append(rowIds, int(record.rowId))
		}
		//recoreds = append(recoreds, *record)
		if cmp < 0 {
			if isInterior {
				// follow left
				crowIds, err := ReadIndexPage(r, record.leftPageNum, key)
				if err != nil {
					return nil, err
				}
				rowIds = append(rowIds, *crowIds...)
				return &rowIds, nil
			} else { // leaf Node
				return &rowIds, nil
			}
		}
	}

	if isInterior {
		return ReadIndexPage(r, rightPageNum, key)
	} else {
		return &rowIds, nil
	}

}

func ReadIndexLeafPage(r io.ReadSeeker, numCell uint16, pageNum uint32, key string) (*[]int, error) {
	return ReadIndexPageBody(r, numCell, pageNum, key, 0, false)
}

func TraverseIndexInteriorPage(r io.ReadSeeker, numCell uint16, pageNum, rightPageNum uint32, key string) (*[]int, error) {
	return ReadIndexPageBody(r, numCell, pageNum, key, rightPageNum, true)
}

func TraverseInteriorPage(r io.ReadSeeker, numCells uint16, pageNum uint32, rightPageNum uint32, rowIds *Q) (*[]Record, error) {

	recs := make([]Record, 0)
	// Read Cell Pointers
	cellPointers := make([]uint16, numCells)
	if err := binary.Read(r, binary.BigEndian, &cellPointers); err != nil {
		fmt.Fprintln(os.Stderr, "error reading cell pointers", err)
		return nil, err
	}

	//childPages := make([]uint32, 0, numCells)

	debug(fmt.Sprintf("numCells : [%d]\n", numCells))

	var rowId int64
	for _, cp := range cellPointers {
		JumpToPage(r, h.pageSize, pageNum, cp)
		var childPageNum uint32
		if err := binary.Read(r, binary.BigEndian, &childPageNum); err != nil {
			fmt.Fprintln(os.Stderr, "error reading child Page Number", err)
			return nil, err
		}
		var err error
		rowId, _, err = ParseVarint(r)
		if err != nil {
			return nil, err
		}

		if rowIds != nil {
			if rowIds.IsEmpty() {
				break
			}
			srowId, _ := rowIds.Top()
			if rowId < int64(srowId) {
				continue
			}
		}

		childRecs, err := ReadTablePage(r, childPageNum, rowIds)
		if err != nil {
			return nil, err
		}
		recs = append(recs, *childRecs...)
	}

	if rowIds != nil {
		if rowIds.IsEmpty() {
			return &recs, nil
		}
		srowId, _ := rowIds.Top()
		if rowId < int64(srowId) {
			return &recs, nil
		}
	}

	cRecs, err := ReadTablePage(r, rightPageNum, rowIds)
	if err != nil {
		return nil, err
	}
	recs = append(recs, *cRecs...)

	debug("\n")

	return &recs, nil
}

type SqlSchemaRecord struct {
	Typ      string
	Name     string
	TablName string
	RootPage uint32
	Sql      string
}

func ReadSqlSchema(r io.ReadSeeker) (*[]SqlSchemaRecord, error) {
	// Assuming the head on page 1 and file header (initial 100 bytes) already consumed / skipped
	records, _ := ReadTablePage(r, 1, nil)
	schemaRecods := make([]SqlSchemaRecord, 0, len(*records))

	for _, rec := range *records {
		schemaRecord := SqlSchemaRecord{
			Typ:      rec.columns[0].StringVal(),
			Name:     rec.columns[1].StringVal(),
			TablName: rec.columns[2].StringVal(),
			RootPage: uint32(rec.columns[3].IntVal()),
			Sql:      rec.columns[4].StringVal(),
		}
		schemaRecods = append(schemaRecods, schemaRecord)
	}
	return &schemaRecods, nil

}

func parseDDL(ddlSql string) *sqlparser.DDL {

	sql := strings.ReplaceAll(ddlSql, "autoincrement", "")
	sql = strings.ReplaceAll(sql, "\"size range\"", "size_range")
	sql = strings.ReplaceAll(sql, "\"", "")

	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Fatalf("Error parsing DDL : {%s} \n Error : %s", sql, err.Error())
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

func fetchDBItemMetaData(recs *[]SqlSchemaRecord, name string) (*SqlSchemaRecord, error) {
	for _, rec := range *recs {
		if rec.Name == name {
			return &rec, nil
		}
	}
	return nil, errors.New("Table not found")
}

var fileHeadLocation int64

func JumpToPage(seeker io.Seeker, pageSize uint16, pageNum uint32, offset uint16) {
	// jump to rootpage
	fileHeadLocation = int64(uint32(pageSize)*(pageNum-1) + uint32(offset))
	_, err := seeker.Seek(fileHeadLocation, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	if pageNum == 50 {
		debug(fmt.Sprintf("Jumped tp <%d - 1> * (%d) + [%d] = %d", pageNum, pageSize, offset, fileHeadLocation))
	}
}

type WhereClause struct {
	Collumn, Value string
}

func ParseWhereClause(selectStmt *sqlparser.Select) *WhereClause {
	where := selectStmt.Where
	if where != nil && where.Type == sqlparser.WhereStr {
		wsplit := strings.Split(sqlparser.String(where.Expr), "=")
		whereColl := strings.TrimSpace(wsplit[0])
		whereVal := strings.Trim(strings.TrimSpace(wsplit[1]), "'")
		// fmt.Printf("Where stmt exists! coll=[%s] val=[%s]\n", whereColl, whereVal)
		return &WhereClause{
			Collumn: whereColl,
			Value:   whereVal,
		}
	}
	return nil
}

func ColumnIndices(ddlStmt *sqlparser.DDL, selectStmt *sqlparser.Select, where *WhereClause) (selectCollIndices []int, whereCollIndex int) {

	selectColumns := make([]string, 0, len(selectStmt.SelectExprs))
	for _, expr := range selectStmt.SelectExprs {
		selectColumns = append(selectColumns, sqlparser.String(expr))
	}

	selectCollIndices = make([]int, 0, len(ddlStmt.TableSpec.Columns))
	whereCollIndex = -1

	collIndexMap := map[string]int{}

	for i, col := range ddlStmt.TableSpec.Columns {
		colName := col.Name.String()
		debug(fmt.Sprintf("column %s (%s) [?pk => %v]\n", col.Name.String(), col.Type.Type, (col.Type.KeyOpt == 1)))
		if strings.ToUpper(col.Type.Type) == "INTEGER" && col.Type.KeyOpt == 1 {
			collIndexMap[colName] = -1 // index for row id
			continue
		}
		collIndexMap[colName] = i
	}

	for _, scoll := range selectColumns {
		selectCollIndices = append(selectCollIndices, collIndexMap[scoll])
	}

	if where != nil {
		whereCollIndex = collIndexMap[where.Collumn]
	}
	return selectCollIndices, whereCollIndex
}

func PrintResult(records *[]Record, selectColumnIndices []int, whereColumnIndex int, where *WhereClause) {
	for _, rec := range *records {
		if where != nil {
			if rec.columns[whereColumnIndex].StringVal() != where.Value {
				// skip it!
				continue
			}
		}
		colVals := make([]string, 0, len(selectColumnIndices))
		for _, colIndex := range selectColumnIndices {
			//debug(fmt.Sprintf("Colun type %x", *rec.columns[colIndex].serialType))
			var colVal string
			if colIndex == -1 {
				colVal = fmt.Sprintf("%d", rec.rowId)
			} else {
				colVal = rec.columns[colIndex].StringVal()
			}
			colVals = append(colVals, colVal)
		}
		fmt.Println(strings.Join(colVals, "|"))
	}
}

func debug(msg string) {
	// fmt.Fprintf(os.Stderr, "DEBUG: %s\n", msg)
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

		metaRecords, err := ReadSqlSchema(databaseFile)
		if err != nil {
			log.Fatal(err.Error())
		}
		// read select Query

		selectQuery := command

		selectStmt := parseSelectQuery(selectQuery)
		tblName := sqlparser.String(selectStmt.From)
		selectColumn := sqlparser.String(selectStmt.SelectExprs)

		tblMetaData, err := fetchDBItemMetaData(metaRecords, tblName)

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
			debug("DDL parsed")
			debug(ddlSql)
			where := ParseWhereClause(selectStmt)

			selectCollIndices, whereCollIndex := ColumnIndices(ddlStmt, selectStmt, where)

			// Use index?

			var rowIdentifiers *Q = nil
			if tblName == "companies" {
				idx, _ := fetchDBItemMetaData(metaRecords, "idx_companies_country")

				rowIds, err := ReadIndexPage(databaseFile, idx.RootPage, where.Value)

				if err != nil {
					log.Fatal("Error reading Index page", err)
				}
				for _, rowId := range *rowIds {
					fmt.Println(fmt.Sprintf("rowId: %v", rowId))
				}

				rowIdentifiers = NewQ(*rowIds)

			}

			// Read data from Table B-Tree
			rootPage := tblMetaData.RootPage
			debug(fmt.Sprintf("Raeding B-Tree root page: <%d>", rootPage))
			recs, err := ReadTablePage(databaseFile, rootPage, rowIdentifiers)
			if err != nil {
				log.Fatal("Error reading page", err)
			}
			PrintResult(recs, selectCollIndices, whereCollIndex, where)
		}
	}
}
