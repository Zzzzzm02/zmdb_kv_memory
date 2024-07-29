package bitcask_go

import "errors"

// 自定义error类型
var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("cannot update the index")
)
