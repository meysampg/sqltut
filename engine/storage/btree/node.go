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

func leafNodeNumCells(order binary.ByteOrder, node []byte) uint32 {
	return order.Uint32(node[LeafNodeNumCellsOffset : LeafNodeNumCellsOffset+LeafNodeNumCellsSize])
}

func leafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) (uint32, *engine.Row) {
	return leafNodeKey(order, node, cellNum), leafNodeValue(order, node, cellNum)
}

func leafNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) uint32 {
	cell := node[offsetOfCell(cellNum):offsetOfCell(cellNum+1)]
	key := cell[LeafNodeKeyOffset : LeafNodeKeyOffset+LeafNodeKeySize]

	return order.Uint32(key)
}

func leafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32) *engine.Row {
	cell := node[offsetOfCell(cellNum):offsetOfCell(cellNum+1)]
	value := cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]

	return utils.Deserialize(order, value)
}

func initializeLeafNode() {

}

func offsetOfCell(cell uint32) uint32 {
	return LeafNodeHeaderSize + cell*LeafNodeCellSize
}
