package btree

import (
	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/utils"
)

type Size struct {
	OffsetSize     uint32
	IdSize         uint32
	UsernameSize   uint32
	EmailSize      uint32
	IdOffset       uint32
	UsernameOffset uint32
	EmailOffset    uint32
	RowSize        uint32
}

func NewSize(row *engine.Row) *Size {
	size := &Size{
		OffsetSize:     utils.SizeOf(uint32(1)),
		IdSize:         utils.SizeOf(row.Id),
		UsernameSize:   utils.SizeOf(row.Username),
		EmailSize:      utils.SizeOf(row.Email),
		IdOffset:       0,
		UsernameOffset: 0,
		EmailOffset:    0,
		RowSize:        0,
	}

	size.IdOffset = 4 * size.OffsetSize // first part is offset size and after that we have id, username and email sizes
	size.UsernameOffset = size.IdOffset + size.IdSize
	size.EmailOffset = size.UsernameOffset + size.UsernameSize
	size.RowSize = 4*size.OffsetSize + size.IdSize + size.UsernameSize + size.EmailSize

	return size
}
