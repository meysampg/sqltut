package btree

import (
	"encoding/binary"

	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/utils"
)

type NodeType string

const (
	NodeInternal NodeType = "node_internal"
	NodeLeaf     NodeType = "node_leaf"
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

	copy(bytes, node[offset:offset+LeafNodeValueSize])
}

func initializeLeafNode(order binary.ByteOrder, node []byte) {
	setLeafNodeNumCells(order, node, 0)
}

func offsetOfCell(cell uint32) uint32 {
	return LeafNodeHeaderSize + cell*LeafNodeCellSize
}
