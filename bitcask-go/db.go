package bitcask_go

import (
	"kv_memory/bitcask-go/data"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	mu *sync.RWMutex
}

func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

}
func (db *DB) appendLogRecord() (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 判断当前活跃文件是否存在
	// 如果为空,则初始化文件
}
