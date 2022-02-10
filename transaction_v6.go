//go:build v6
// +build v6

package gorocksdb

// #include "rocksdb/c.h"
import "C"

import (
	"errors"
	"runtime"
	"unsafe"
)

import "C"

// GetForUpdate queries the data associated with the key and puts an exclusive lock on the key from the database given this transaction and column family.
func (transaction *Transaction) GetForUpdateCF(opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_transaction_get_for_update_cf(
		transaction.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, C.uchar(byte(1)) /*exclusive*/, &cErr,
	)
	runtime.KeepAlive(key)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// Merge merges the data associated with the key with the actual data in the database.
func (transaction *Transaction) MergeCF(cf *ColumnFamilyHandle, key []byte, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_transaction_merge_cf(transaction.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}
