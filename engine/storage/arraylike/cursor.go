package arraylike

import "github.com/meysampg/sqltut/engine"

type Cursor struct {
	Table      engine.Storage
	RowNum     uint32
	EndOfTable bool
}

func TableStart(storage engine.Storage) *Cursor {
	return &Cursor{
		Table:      storage,
		RowNum:     0,
		EndOfTable: storage.RowNums() == 0,
	}
}

func TableEnd(storage engine.Storage) *Cursor {
	return &Cursor{
		Table:      storage,
		RowNum:     storage.RowNums(),
		EndOfTable: true,
	}
}

func (c *Cursor) Advance() {
	c.RowNum++

	if c.RowNum >= c.Table.RowNums() {
		c.EndOfTable = true
	}
}
