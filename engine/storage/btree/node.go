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

const (
	/*
	 * Split Count - We use the half-half strategy for the time being
	 **/
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSplitCount  = (LeafNodeMaxCells + 1) - LeafNodeRightSplitCount
)

const (
	/*
	 * Internal Node Header Layout
	 */
	InternalNodeNumKeysSize      = 4
	InternalNodeNumKeysOffset    = CommonNodeHeaderSize
	InternalNodeRightChildSize   = 4
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       = CommonNodeHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

const (
	/*
	 * Internal Node Body Layout
	 */
	InternalNodeKeyOffset   = 0
	InternalNodeKeySize     = 4
	InternalNodeChildOffset = InternalNodeKeyOffset + InternalNodeKeySize
	InternalNodeChildSize   = 4
	InternalNodeCellSize    = InternalNodeChildSize + InternalNodeKeySize
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

func leafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	return node[offsetOfLeafCell(cellNum):offsetOfLeafCell(cellNum+1)]
}

func getLeafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) (uint32, *engine.Row) {
	return getLeafNodeKey(order, node, cellNum), getLeafNodeValue(order, node, cellNum)
}

func setLeafNodeCell(order binary.ByteOrder, node []byte, cellNum uint32, key uint32, value *engine.Row) {
	setLeafNodeKey(order, node, cellNum, key)
	setLeafNodeValue(order, node, cellNum, value)
}

func leafNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	cell := node[offsetOfLeafCell(cellNum):offsetOfLeafCell(cellNum+1)]
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
	cell := node[offsetOfLeafCell(cellNum):offsetOfLeafCell(cellNum+1)]
	value := cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]

	return value
}

func getLeafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32) *engine.Row {
	return utils.Deserialize(order, leafNodeValue(order, node, cellNum))
}

func setLeafNodeValue(order binary.ByteOrder, node []byte, cellNum uint32, row *engine.Row) {
	bytes := utils.Serialize(order, row)
	offset := offsetOfLeafCell(cellNum) + LeafNodeValueOffset

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

func isNodeRoot(order binary.ByteOrder, node []byte) []byte {
	return node[IsRootOffset : IsRootOffset+IsRootSize]
}

func getIsNodeRoot(order binary.ByteOrder, node []byte) bool {
	return isNodeRoot(order, node)[0] == 1
}

func setIsNodeRoot(order binary.ByteOrder, node []byte, isRoot bool) {
	b2i := map[bool]byte{false: 0, true: 1}
	copy(isNodeRoot(order, node), []byte{b2i[isRoot]})
}

func createNewRoot(table *Table, rightChildPageNum uint32) (engine.ExecutionStatus, error) {
	root, err := table.pager.GetPage(table.rootPageNum)
	if err != nil {
		return engine.ExitFailure, err
	}

	_, err = table.pager.GetPage(rightChildPageNum)
	if err != nil {
		return engine.ExitFailure, err
	}

	leftChildPageNum, err := getUnusedPageNum(table.pager)
	if err != nil {
		return 0, err
	}

	leftChild, err := table.pager.GetPage(leftChildPageNum)
	if err != nil {
		return 0, err
	}

	copy(leftChild, root) // copy root to left child
	setIsNodeRoot(Orderness, leftChild, false)

	initializeInternalNode(Orderness, root)
	setIsNodeRoot(Orderness, root, true)
	setInternalNodeNumKeys(Orderness, root, 1)
	status, err := setInternalNodeChild(Orderness, root, 0, leftChildPageNum)
	if err != nil {
		return status, err
	}
	leftChildMaxKey := getNodeMaxKey(Orderness, leftChild)
	setInternalNodeKey(Orderness, root, 0, leftChildMaxKey)
	setInternalNodeRightChild(Orderness, root, rightChildPageNum)

	return engine.ExecuteSuccess, nil
}

func initializeLeafNode(order binary.ByteOrder, node []byte) {
	initializeNode(order, node, NodeLeaf, false, 0)
}

func initializeInternalNode(order binary.ByteOrder, node []byte) {
	initializeNode(order, node, NodeInternal, false, 0)
}

func initializeNode(order binary.ByteOrder, node []byte, typ NodeType, isRoot bool, numCells uint32) {
	setNodeType(order, node, typ)
	setIsNodeRoot(order, node, isRoot)
	setLeafNodeNumCells(order, node, numCells)
}

func internalNodeCell(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	return node[offsetOfInternalCell(cellNum):offsetOfInternalCell(cellNum+1)]
}

func getInternalNodeCell(order binary.ByteOrder, node []byte, childNum uint32) uint32 {
	return order.Uint32(internalNodeCell(order, node, childNum))
}

func internalNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) []byte {
	return internalNodeCell(order, node, cellNum)[InternalNodeKeyOffset : InternalNodeKeyOffset+InternalNodeKeySize]
}

func getInternalNodeKey(order binary.ByteOrder, node []byte, cellNum uint32) uint32 {
	return order.Uint32(internalNodeKey(order, node, cellNum))
}

func setInternalNodeKey(order binary.ByteOrder, node []byte, cellNum uint32, key uint32) {
	order.PutUint32(internalNodeKey(order, node, cellNum), key)
}

func internalNodeNumKeys(order binary.ByteOrder, node []byte) []byte {
	return node[InternalNodeNumKeysOffset : InternalNodeNumKeysOffset+InternalNodeNumKeysSize]
}

func getInternalNodeNumKeys(order binary.ByteOrder, node []byte) uint32 {
	return order.Uint32(internalNodeNumKeys(order, node))
}

func setInternalNodeNumKeys(order binary.ByteOrder, node []byte, numKeys uint32) {
	order.PutUint32(internalNodeNumKeys(order, node), numKeys)
}

func internalNodeRightChild(order binary.ByteOrder, node []byte) []byte {
	return node[InternalNodeRightChildOffset : InternalNodeRightChildOffset+InternalNodeRightChildSize]
}

func getInternalNodeRightChild(order binary.ByteOrder, node []byte) uint32 {
	return order.Uint32(internalNodeRightChild(order, node))
}

func setInternalNodeRightChild(order binary.ByteOrder, node []byte, pageNum uint32) {
	order.PutUint32(internalNodeRightChild(order, node), pageNum)
}

func internalNodeChild(order binary.ByteOrder, node []byte, childNum uint32) ([]byte, engine.ExecutionStatus, error) {
	numKeys := getInternalNodeNumKeys(order, node)
	if childNum > numKeys {
		return nil, engine.ExitFailure, fmt.Errorf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
	} else if childNum == numKeys {
		return internalNodeRightChild(order, node), 0, nil
	}

	return internalNodeCell(order, node, childNum), 0, nil
}

func getInternalNodeChild(order binary.ByteOrder, node []byte, childNum uint32) (uint32, engine.ExecutionStatus, error) {
	bytes, status, err := internalNodeChild(order, node, childNum)
	if err != nil || status != 0 {
		return 0, status, err
	}

	return order.Uint32(bytes), 0, nil
}

func setInternalNodeChild(order binary.ByteOrder, node []byte, childNum uint32, pageNum uint32) (engine.ExecutionStatus, error) {
	bytes, status, err := internalNodeChild(order, node, childNum)
	if err != nil || status != 0 {
		return status, err
	}

	order.PutUint32(bytes, pageNum)

	return 0, nil
}

func getNodeMaxKey(order binary.ByteOrder, node []byte) uint32 {
	switch getNodeType(order, node) {
	case NodeInternal:
		return getInternalNodeKey(order, node, getInternalNodeNumKeys(order, node)-1)
	case NodeLeaf:
		return getLeafNodeKey(order, node, getLeafNodeNumCells(order, node)-1)
	default:
		return 0 // node types are sealed, just for pass
	}
}

func leafNodeInsert(c *cursor, key uint32, value *engine.Row) (engine.ExecutionStatus, error) {
	node, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return engine.ExitFailure, err
	}

	numCells := getLeafNodeNumCells(Orderness, node)
	if numCells >= LeafNodeMaxCells {
		return leafNodeSplitAndInsert(c, key, value)
	}

	if c.cellNum < numCells {
		if getLeafNodeKey(Orderness, node, c.cellNum) == key {
			return engine.ExecuteDuplicateKey, nil
		}

		var i uint32
		for i = numCells; i > c.cellNum; i-- {
			copy(node[offsetOfLeafCell(i):offsetOfLeafCell(i+1)], node[offsetOfLeafCell(i-1):offsetOfLeafCell(i)])
		}
	}

	setLeafNodeNumCells(Orderness, node, getLeafNodeNumCells(Orderness, node)+1)
	setLeafNodeCell(Orderness, node, c.cellNum, key, value)

	return engine.ExecuteSuccess, nil
}

func leafNodeSplitAndInsert(c *cursor, key uint32, value *engine.Row) (engine.ExecutionStatus, error) {
	oldPage, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return 0, err
	}

	newPageNum, err := getUnusedPageNum(c.table.pager)
	if err != nil {
		return 0, err
	}

	newPage, err := c.table.pager.GetPage(newPageNum)
	if err != nil {
		return 0, err
	}

	initializeLeafNode(Orderness, newPage)

	// start from the top level, we put upper cells into new node, the new insert
	// in new or old node and let lower cells to remain on the old node.
	var destination []byte
	for i := LeafNodeMaxCells; i != 0; i-- {
		if i >= LeafNodeLeftSplitCount {
			destination = newPage
		} else {
			destination = oldPage
		}

		cellNum := i % LeafNodeLeftSplitCount
		cell := leafNodeCell(Orderness, oldPage, cellNum)

		if c.cellNum == i { // new element to insert
			setLeafNodeCell(Orderness, destination, cellNum, key, value)
		} else if c.cellNum > i { // is not new element and just put elements from old page to old page
			copy(leafNodeCell(Orderness, destination, cellNum), cell)
		} else { // is not new element and copy from old page to new page
			copy(leafNodeCell(Orderness, destination, cellNum-1), cell)
		}
	}

	setLeafNodeNumCells(Orderness, newPage, LeafNodeLeftSplitCount)
	setLeafNodeNumCells(Orderness, oldPage, LeafNodeRightSplitCount)

	if getIsNodeRoot(Orderness, oldPage) {
		return createNewRoot(c.table, newPageNum)
	}

	return engine.ExitFailure, fmt.Errorf("Need to implement updating parent after split")
}

func getUnusedPageNum(p *Pager) (uint32, error) {
	return p.numPages, nil
}

func offsetOfLeafCell(cell uint32) uint32 {
	return LeafNodeHeaderSize + cell*LeafNodeCellSize
}

func offsetOfInternalCell(cell uint32) uint32 {
	return InternalNodeHeaderSize + cell*InternalNodeCellSize
}
