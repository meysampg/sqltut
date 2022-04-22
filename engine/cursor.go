package engine

type Cursor struct {
	Table      Storage
	RowNum     uint32
	EndOfTable bool
}

func TableStart(storage Storage) *Cursor {
	return &Cursor{
		Table:      storage,
		RowNum:     0,
		EndOfTable: storage.RowNums() == 0,
	}
}

func TableEnd(storage Storage) *Cursor {
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
