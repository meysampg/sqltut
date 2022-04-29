package utils

import (
	"reflect"
	"unsafe"

	"github.com/meysampg/sqltut/engine"
)

func SizeOf(i interface{}) uint32 {
	v := reflect.Indirect(reflect.ValueOf(i))

	switch v.Kind() {
	case reflect.String:
		s := v.String()
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))

		return uint32(hdr.Len)

	case reflect.Bool,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Int, reflect.Uint,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return uint32(v.Type().Size())

	default:
		return 0
	}
}

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
		OffsetSize:     SizeOf(uint32(1)),
		IdSize:         SizeOf(row.Id),
		UsernameSize:   SizeOf(row.Username),
		EmailSize:      SizeOf(row.Email),
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
