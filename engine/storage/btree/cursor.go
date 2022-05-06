package btree

import (
	"fmt"
)

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

func tableFind(table *Table, key uint32) (*cursor, error) {
	node, err := table.pager.GetPage(table.rootPageNum)
	if err != nil {
		return nil, err
	}

	if getNodeType(Orderness, node) == NodeLeaf {
		return leafNodeFind(table, table.rootPageNum, key)
	}

	return nil, fmt.Errorf("Need to implement searching an internal node")
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) (*cursor, error) {
	node, err := table.pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}

	cur := &cursor{
		table:   table,
		pageNum: pageNum,
	}

	var minIndex uint32
	lastIndex := getLeafNodeNumCells(Orderness, node)

	for minIndex != lastIndex {
		index := (minIndex + lastIndex) / 2
		keyAtIndex := getLeafNodeKey(Orderness, node, index)
		if keyAtIndex == key {
			minIndex = index
			break
		}

		if key < keyAtIndex {
			lastIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cur.cellNum = minIndex

	return cur, nil
}

func cursorValue(cursor *cursor) ([]byte, error) {
	page, err := cursor.table.pager.GetPage(cursor.pageNum)
	if err != nil {
		return nil, err
	}

	return leafNodeValue(Orderness, page, cursor.cellNum), nil
}
