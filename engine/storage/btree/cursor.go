package btree

type cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool
}

func (c *cursor) Advance() error {
	page, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return err
	}

	c.cellNum += 1

	if c.cellNum >= getLeafNodeNumCells(Orderness, page) {
		c.endOfTable = true
	}

	return nil
}

func tableStart(table *Table) (*cursor, error) {
	rootPage, err := table.pager.GetPage(table.rootPageNum)
	if err != nil {
		return nil, err
	}
	numCells := getLeafNodeNumCells(Orderness, rootPage)

	return &cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		cellNum:    0,
		endOfTable: numCells == 0,
	}, nil
}

func tableEnd(table *Table) (*cursor, error) {
	rootPage, err := table.pager.GetPage(table.rootPageNum)
	if err != nil {
		return nil, err
	}
	numCells := getLeafNodeNumCells(Orderness, rootPage)

	return &cursor{
		table:      table,
		pageNum:    table.rootPageNum,
		cellNum:    numCells,
		endOfTable: true,
	}, nil
}

func cursorValue(cursor *cursor) ([]byte, error) {
	page, err := cursor.table.pager.GetPage(cursor.pageNum)
	if err != nil {
		return nil, err
	}

	return leafNodeValue(Orderness, page, cursor.cellNum), nil
}
