package utils

import (
	"reflect"
	"unsafe"
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
