package arraylike

type cursor struct {
	table      *Table
	rowNum     uint32
	endOfTable bool
}

func tableStart(table *Table) *cursor {
	return &cursor{
		table:      table,
		rowNum:     0,
		endOfTable: table.RowNums() == 0,
	}
}

func tableEnd(table *Table) *cursor {
	return &cursor{
		table:      table,
		rowNum:     table.RowNums(),
		endOfTable: true,
	}
}

func (c *cursor) Advance() error {
	c.rowNum++

	if c.rowNum >= c.table.RowNums() {
		c.endOfTable = true
	}

	return nil // BC
}
