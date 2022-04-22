package btree

import (
	"encoding/binary"

	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/utils"
)

func Serialize(enc binary.ByteOrder, row *engine.Row) []byte {
	var serializedRow []byte
	size := NewSize(row)
	if size.RowSize == 0 {
		return serializedRow
	}

	serializedRow = make([]byte, size.RowSize, size.RowSize)
	// offset size
	enc.PutUint32(serializedRow[:size.OffsetSize], size.OffsetSize)
	// id size
	enc.PutUint32(serializedRow[size.OffsetSize:2*size.OffsetSize], size.IdSize)
	// username size
	enc.PutUint32(serializedRow[2*size.OffsetSize:3*size.OffsetSize], size.UsernameSize)
	// email size
	enc.PutUint32(serializedRow[3*size.OffsetSize:4*size.OffsetSize], size.EmailSize)

	// id
	enc.PutUint32(serializedRow[size.IdOffset:size.UsernameOffset], row.Id)
	// username
	copy(serializedRow[size.UsernameOffset:size.EmailOffset], row.Username)
	// email
	copy(serializedRow[size.EmailOffset:], row.Email)

	return serializedRow
}

func Deserialize(dec binary.ByteOrder, data []byte) *engine.Row {
	if len(data) == 0 {
		return nil
	}
	row := &engine.Row{}

	offsetSize := dec.Uint32(data[:utils.SizeOf(uint32(1))])
	idSize := dec.Uint32(data[offsetSize : 2*offsetSize])
	idOffset := 4 * offsetSize
	usernameSize := dec.Uint32(data[2*offsetSize : 3*offsetSize])
	usernameOffset := idOffset + idSize
	emailSize := dec.Uint32(data[3*offsetSize : 4*offsetSize])
	emailOffset := usernameOffset + usernameSize

	// id
	row.Id = dec.Uint32(data[idOffset:usernameOffset])
	// email
	row.Username = string(data[usernameOffset:emailOffset])
	// username
	row.Email = string(data[emailOffset : emailOffset+emailSize])

	return row
}
