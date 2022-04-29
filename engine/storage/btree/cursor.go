package btree

type cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool
}

func (c *cursor) Advance() error {
	_, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return err
	}

	c.pageNum += 1

	// TODO

	return nil
}

func tableStart(table *Table) *cursor {
	return &cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		cellNum:    0,
		endOfTable: false,
	}
}

func tableEnd(table *Table) *cursor {
	return &cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		cellNum:    0,
		endOfTable: false,
	}
}

func cursorValue(cursor *cursor) ([]byte, uint32, error) {
	page, err := cursor.table.pager.GetPage(cursor.pageNum)
	if err != nil {
		return nil, 0, err
	}

	return page, 0, nil
}
