package arraylike

import (
	"encoding/binary"
	"fmt"

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
	Pager   *Pager
}

func DbOpen(filename string) (*Table, error) {
	pager, err := NewPager(filename)
	if err != nil {
		return nil, err
	}

	return &Table{
		NumRows: pager.FileLength / RowSize,
		Pager:   pager,
	}, nil
}

func (t *Table) RowNums() uint32 {
	return t.NumRows
}

func (t *Table) GetPager() engine.Pager {
	return t.Pager
}

func (t *Table) Close() (engine.ExecutionStatus, error) {
	pager := t.Pager

	// flush pages and clean-up them
	numFullPages := t.NumRows / RowsPerPage
	for i := 0; i < int(numFullPages); i++ {
		if pager.Pages[i] == nil {
			continue
		}
		if err := pager.Flush(i, PageSize); err != nil {
			return engine.ExitFailure, err
		}
		pager.Pages[i] = nil
	}

	// if we have partial page, we should write them to disk too.
	numAdditionalRows := t.NumRows % RowsPerPage
	if numAdditionalRows > 0 && pager.Pages[numFullPages] != nil { // partial page only can be occurred on the last page
		if err := pager.Flush(int(numFullPages), numAdditionalRows*RowSize); err != nil {
			return engine.ExitFailure, err
		}
		pager.Pages[int(numFullPages)] = nil
	}

	// close the DB file
	if err := pager.FileDescriptor.Close(); err != nil {
		return engine.ExitFailure, fmt.Errorf("Error closing db file.")
	}

	for i := 0; i < int(TableMaxPage); i++ {
		if pager.Pages[i] != nil {
			pager.Pages[i] = nil
		}
	}

	return engine.ExecuteSuccess, nil
}

func cursorValue(cursor *Cursor) ([]byte, uint32, error) {
	rowNum := cursor.RowNum
	pageNum := rowNum / RowsPerPage
	page, err := cursor.Table.GetPager().GetPage(pageNum)
	if err != nil {
		return nil, 0, err
	}
	rowOffset := rowNum % RowsPerPage
	byteOffset := rowOffset * RowSize

	return page, byteOffset, nil
}

func (t *Table) Insert(row *engine.Row) engine.ExecutionStatus {
	if t.NumRows > TableMaxPage {
		return engine.ExecuteTableFull
	}

	cursor := TableEnd(t)
	page, byteOffset, err := cursorValue(cursor)
	if err != nil {
		fmt.Println(err)
		return engine.ExecutePageFetchError
	}
	serializedRow := Serialize(binary.LittleEndian, row)

	copy(page[byteOffset:], serializedRow)

	t.NumRows++

	return engine.ExecuteSuccess
}

func (t *Table) Select() ([]*engine.Row, engine.ExecutionStatus) {
	var result []*engine.Row
	cursor := TableStart(t)
	for !cursor.EndOfTable {
		page, byteOffset, err := cursorValue(cursor)
		if err != nil {
			fmt.Println(err)
			return nil, engine.ExecutePageFetchError
		}
		row := Deserialize(binary.LittleEndian, page[byteOffset:byteOffset+RowSize])
		if row == nil {
			return nil, engine.ExecuteRowNotFound
		}
		result = append(result, row)
		cursor.Advance()
	}

	return result, engine.ExecuteSuccess
}
