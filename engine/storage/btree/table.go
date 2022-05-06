package btree

import (
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
	rootPageNum uint32
	pager       *Pager
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
		rootNode, err := pager.GetPage(0)
		if err != nil {
			return nil, err
		}

		initializeLeafNode(Orderness, rootNode)
		setIsNodeRoot(Orderness, rootNode, true)
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

	for i := 0; i < numPages; i++ {
		if pager.pages[i] != nil {
			pager.pages[i] = nil
		}
	}

	return engine.ExecuteSuccess, nil
}

func (t *Table) Insert(row *engine.Row) engine.ExecutionStatus {
	cursor, err := tableFind(t, row.Id)
	if err != nil {
		if err.Error() == "Need to implement searching an internal node" {
			return engine.TODO
		}

		return engine.ExecutePageFetchError
	}

	status, err := leafNodeInsert(cursor, row.Id, row)
	if err != nil {
		fmt.Println(err)
	}

	return status
}

func (t *Table) Select() ([]*engine.Row, engine.ExecutionStatus) {
	var result []*engine.Row
	cursor, err := tableStart(t)
	if err != nil {
		return nil, engine.ExecutePageFetchError
	}
	for !cursor.endOfTable {
		page, err := cursorValue(cursor)
		if err != nil {
			fmt.Println(err)
			return nil, engine.ExecutePageFetchError
		}
		row := utils.Deserialize(Orderness, page)
		if row == nil {
			return nil, engine.ExecuteRowNotFound
		}
		result = append(result, row)
		err = cursor.Advance()
		if err != nil {
			fmt.Println(err)
			return nil, engine.ExecutePageFetchError
		}
	}

	return result, engine.ExecuteSuccess
}

func (t *Table) ExecuteMeta(command []byte) engine.ExecutionStatus {
	if engine.Equal(command, ".constants") {
		fmt.Println("Constants:")
		printConstants()

		return engine.MetaCommandSuccess
	} else if engine.Equal(command, ".btree") {
		fmt.Println("Tree:")
		printTree(t.pager, 0, 0)

		return engine.MetaCommandSuccess
	}

	return engine.MetaUnrecognizedCommand
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", RowSize)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", CommonNodeHeaderSize)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNodeHeaderSize)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNodeCellSize)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LeafNodeSpaceForCells)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Print("  ")
	}
}

func printTree(pager *Pager, pageNum uint32, indentationLevel uint32) {
	node, _ := pager.GetPage(pageNum)
	var numKeys, child uint32

	switch getNodeType(Orderness, node) {
	case NodeLeaf:
		numKeys = getLeafNodeNumCells(Orderness, node)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)
			fmt.Printf("- %d\n", getLeafNodeKey(Orderness, node, i))
		}
	case NodeInternal:
		numKeys = getInternalNodeNumKeys(Orderness, node)
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child, _, _ = getInternalNodeChildPage(Orderness, node, i)
			printTree(pager, child, indentationLevel+1)

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", getInternalNodeKey(Orderness, node, i))
		}
		child = getInternalNodeRightChild(Orderness, node)
		printTree(pager, child, indentationLevel+1)
	}
}
