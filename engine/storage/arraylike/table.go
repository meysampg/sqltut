package arraylike

import (
	"encoding/binary"
	"github.com/meysampg/sqltut/engine"
)

const (
	PageSize     uint32 = 4096
	TableMaxPage uint32 = 100
	RowSize      uint32 = 4 + 255 + 255 // for the time being we assume string as a VARCHAR[255]
	RowsPerPage  uint32 = PageSize / RowSize
	TableMaxRows uint32 = RowsPerPage * TableMaxPage
)

type Table struct {
	NumRows uint32
	Pages   [][]byte
}

func NewTable() *Table {
	return &Table{
		NumRows: 0,
		Pages:   make([][]byte, TableMaxPage, TableMaxPage),
	}
}

func (t *Table) rowSlot() ([]byte, uint32) {
	pageNum := t.NumRows / RowsPerPage
	if t.Pages[pageNum] == nil {
		t.Pages[pageNum] = make([]byte, PageSize, PageSize)
	}
	page := t.Pages[pageNum]

	rowOffset := t.NumRows % RowsPerPage
	byteOffset := rowOffset * RowSize

	return page, byteOffset
}

func (t *Table) Insert(row *engine.Row) engine.ExecutionStatus {
	if t.NumRows > TableMaxPage {
		return engine.ExecuteTableFull
	}

	page, byteOffset := t.rowSlot()
	serializedRow := Serialize(binary.LittleEndian, row)

	copy(page[byteOffset:], serializedRow)

	t.NumRows++

	return engine.ExecuteSuccess
}
