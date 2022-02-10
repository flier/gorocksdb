//go:build v6
// +build v6

package gorocksdb

// #include "rocksdb/c.h"
import "C"

func (env *Env) LowerThreadPoolIOPriority() {
	C.rocksdb_env_lower_thread_pool_io_priority(env.c)
}

func (env *Env) LowerHighPriorityThreadPoolIOPriority() {
	C.rocksdb_env_lower_high_priority_thread_pool_io_priority(env.c)
}

func (env *Env) LowerThreadPoolCPUPriority() {
	C.rocksdb_env_lower_thread_pool_cpu_priority(env.c)
}

func (env *Env) LowerHighPriorityThreadPoolCPUPriority() {
	C.rocksdb_env_lower_high_priority_thread_pool_cpu_priority(env.c)
}
