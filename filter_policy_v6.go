//go:build v6
// +build v6

package gorocksdb

// #include "rocksdb/c.h"
import "C"

// NewBloomFilter returns a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~1% false positive rate.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
func NewBloomFilter(bitsPerKey int) FilterPolicy {
	return NewNativeFilterPolicy(C.rocksdb_filterpolicy_create_bloom(C.double(bitsPerKey)))
}

// NewBloomFilterFull returns a new filter policy created with use_block_based_builder=false
// (use full or partitioned filter).
func NewBloomFilterFull(bitsPerKey int) FilterPolicy {
	return NewNativeFilterPolicy(C.rocksdb_filterpolicy_create_bloom_full(C.double(bitsPerKey)))
}
