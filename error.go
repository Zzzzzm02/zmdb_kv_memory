package kv_memory

import "errors"

// 自定义error类型
var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("cannot update the index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDatafileNotFound       = errors.New("datafile not found")
	ErrDataDirectorycorrupted = errors.New("data directory corrupted")
)
