package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/utils"
)

type NodeType uint8

const (
	NodeInternal NodeType = 0
	NodeLeaf     NodeType = 1
)

const (
	/*
	* Common Node Header Layout
	**/
	NodeTypeSize         = 1
	NodeTypeOffset       = 0
	IsRootSize           = 1
	IsRootOffset         = NodeTypeSize
	ParentPointerSize    = 4
	ParentPointerOffset  = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)
const (
	/*
	 * Leaf Node Header Layout
	 **/
	LeafNodeNumCellsSize   = 4
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

const (
	/*
	 * Leaf Node Body Layout
	 **/
	LeafNodeKeySize       = 4
	LeafNodeKeyOffset     = 0
	LeafNodeValueSize     = RowSize
	LeafNodeValueOffset   = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize      = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells = PageSize - uint32(LeafNodeHeaderSize)
	LeafNodeMaxCells      = LeafNodeSpaceForCells / LeafNodeCellSize
)

var Orderness binary.ByteOrder = binary.LittleEndian

func leafNodeNumCells(order binary.ByteOrder, node []byte) []byte {
	return node[LeafNodeNumCellsOffset : LeafNodeNumCellsOffset+LeafNodeNumCellsSize]
}

func setLeafNodeNumCells(order binary.ByteOrder, node []byte, value uint32) {
	order.PutUint32(leafNodeNumCells(order, node), value)
}

func getLeafNodeNumCells(order binary.ByteOrder, node []byte) uint32 {
	return order.Uint32(leafNodeNumCells(order, node))
}

func leafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) ([]byte, []byte) {
	return leafNodeKey(order, node, cellNum), leafNodeValue(order, node, cellNum)
}

func getLeafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) (uint32, *engine.Row) {
	return getLeafNodeKey(order, node, cellNum), getLeafNodeValue(order, node, cellNum)
}

func setLeafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32, key uint32, value *engine.Row) {
	setLeafNodeKey(order, node, cellNum, key)
	setLeafNodeValue(order, node, cellNum, value)
}

func leafNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	cell := node[offsetOfCell(cellNum):offsetOfCell(cellNum+1)]
	key := cell[LeafNodeKeyOffset : LeafNodeKeyOffset+LeafNodeKeySize]

	return key
}

func setLeafNodeKey(order binary.ByteOrder, node []byte, cellNum uint32, value uint32) {
	order.PutUint32(leafNodeKey(order, node, cellNum), value)
}

func getLeafNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) uint32 {
	return order.Uint32(leafNodeKey(order, node, cellNum))
}

func leafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	cell := node[offsetOfCell(cellNum):offsetOfCell(cellNum+1)]
	value := cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]

	return value
}

func getLeafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32) *engine.Row {
	return utils.Deserialize(order, leafNodeValue(order, node, cellNum))
}

func setLeafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32, row *engine.Row) {
	bytes := utils.Serialize(order, row)
	offset := offsetOfCell(cellNum) + LeafNodeValueOffset

	copy(node[offset:offset+LeafNodeValueSize], bytes)
}

func nodeType(order binary.ByteOrder, node []byte) []byte {
	return node[NodeTypeOffset : NodeTypeOffset+NodeTypeSize]
}

func getNodeType(order binary.ByteOrder, node []byte) NodeType {
	return NodeType(nodeType(order, node)[0])
}

func setNodeType(order binary.ByteOrder, node []byte, typ NodeType) {
	copy(nodeType(order, node), []byte{byte(typ)})
}

func initializeLeafNode(order binary.ByteOrder, node []byte) {
	setNodeType(order, node, NodeLeaf)
	setLeafNodeNumCells(order, node, 0)
}

func leafNodeInsert(c *cursor, key uint32, value *engine.Row) (engine.ExecutionStatus, error) {
	node, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return engine.ExitFailure, err
	}

	numCells := getLeafNodeNumCells(Orderness, node)
	if numCells >= LeafNodeMaxCells {
		return engine.ExitFailure, fmt.Errorf("Need to implement splitting a leaf node.")
	}

	if c.cellNum < numCells {
		if getLeafNodeKey(Orderness, node, c.cellNum) == key {
			return engine.ExecuteDuplicateKey, nil
		}

		var i uint32
		for i = numCells; i > c.cellNum; i-- {
			copy(node[offsetOfCell(i):offsetOfCell(i+1)], node[offsetOfCell(i-1):offsetOfCell(i)])
		}
	}

	setLeafNodeNumCells(Orderness, node, getLeafNodeNumCells(Orderness, node)+1)
	setLeafNodeCell(Orderness, node, c.cellNum, key, value)

	return engine.ExecuteSuccess, nil
}

func offsetOfCell(cell uint32) uint32 {
	return LeafNodeHeaderSize + cell*LeafNodeCellSize
}
