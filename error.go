package kv_memory

import "errors"

// 自定义error类型
var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("cannot update the index")
	ErrDirIsEmpty        = errors.New("the directory is empty")
)
