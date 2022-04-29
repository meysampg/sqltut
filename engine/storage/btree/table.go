package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/utils"
)

const (
	PageSize     uint32 = 4096
	TableMaxPage uint32 = 100
	RowSize      uint32 = 4 + 255 + 255 // for the time being we assume string as a VARCHAR[255]
)

type Table struct {
	NumRows     uint32
	RootPageNum uint32
	Pager       *Pager
}

func DbOpen(filename string) (*Table, error) {
	pager, err := NewPager(filename)
	if err != nil {
		return nil, err
	}

	t := &Table{
		rootPageNum: 0,
		pager:       pager,
	}

	if pager.numPages == 0 {
		// TODO initialize
	}

	return t, nil
}

func (t *Table) GetPager() engine.Pager {
	return t.pager
}

func (t *Table) Close() (engine.ExecutionStatus, error) {
	pager := t.pager

	// flush pages and clean-up them
	numPages := int(pager.GetNumPages())
	for i := 0; i < numPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		if err := pager.Flush(i, PageSize); err != nil {
			return engine.ExitFailure, err
		}
		pager.pages[i] = nil
	}

	// close the DB file
	if err := pager.fileDescriptor.Close(); err != nil {
		return engine.ExitFailure, fmt.Errorf("Error closing db file.")
	}

	for i := 0; i < int(TableMaxPage); i++ {
		if pager.pages[i] != nil {
			pager.pages[i] = nil
		}
	}

	return engine.ExecuteSuccess, nil
}

func (t *Table) Insert(row *engine.Row) engine.ExecutionStatus {
	//if t.numRows > TableMaxPage {
	//	return engine.ExecuteTableFull
	//}

	cursor := tableEnd(t)
	page, byteOffset, err := cursorValue(cursor)
	if err != nil {
		fmt.Println(err)
		return engine.ExecutePageFetchError
	}
	serializedRow := utils.Serialize(binary.LittleEndian, row)

	copy(page[byteOffset:], serializedRow)

	//t.NumRows++

	return engine.ExecuteSuccess
}

func (t *Table) Select() ([]*engine.Row, engine.ExecutionStatus) {
	var result []*engine.Row
	cursor := tableStart(t)
	for !cursor.endOfTable {
		page, byteOffset, err := cursorValue(cursor)
		if err != nil {
			fmt.Println(err)
			return nil, engine.ExecutePageFetchError
		}
		row := utils.Deserialize(binary.LittleEndian, page[byteOffset:byteOffset+RowSize])
		if row == nil {
			return nil, engine.ExecuteRowNotFound
		}
		result = append(result, row)
		cursor.Advance()
	}

	return result, engine.ExecuteSuccess
}
