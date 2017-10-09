package gorocksdb

import "C"
import (
	"reflect"
	"unsafe"
)

// btoi converts a bool value to int.
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// boolToChar converts a bool value to C.uchar.
func boolToChar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

// charToByte converts a *C.char to a byte slice.
func charToByte(data *C.char, len C.size_t) []byte {
	var value []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// byteToChar returns *C.char from byte slice.
func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}
	return c
}

func byteSliceToArray(vals [][]byte) (**C.char, *C.size_t) {
	if len(vals) == 0 {
		return nil, nil
	}

	chars := make([]*C.char, len(vals))
	sizes := make([]C.size_t, len(vals))
	for i, val := range vals {
		chars[i] = byteToChar(val)
		sizes[i] = C.size_t(len(val))
	}

	cCharBuf := C.malloc(C.size_t(unsafe.Sizeof(chars[0])) * C.size_t(len(chars)))
	copy(((*[1 << 32]*C.char)(cCharBuf))[:], chars)

	cChars := (**C.char)(cCharBuf)

	cSizes := (*C.size_t)(unsafe.Pointer(&sizes[0]))
	return cChars, cSizes

}

// Go []byte to C string
// The C string is allocated in the C heap using malloc.
func cByteSlice(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		cData := C.malloc(C.size_t(len(b)))
		copy((*[1 << 24]byte)(cData)[0:len(b)], b)
		c = (*C.char)(cData)
	}
	return c
}

// stringToChar returns *C.char from string.
func stringToChar(s string) *C.char {
	ptrStr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(ptrStr.Data))
}

// charSlice converts a C array of *char to a []*C.char.
func charSlice(data **C.char, len C.int) []*C.char {
	var value []*C.char
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// sizeSlice converts a C array of size_t to a []C.size_t.
func sizeSlice(data *C.size_t, len C.int) []C.size_t {
	var value []C.size_t
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}
