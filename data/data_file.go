package data

import (
	"kv_memory/fio"
)

// DataFile 数据文件结构
type DataFile struct {
	FileId    uint32        // 当前文件Id
	WriteOff  int64         // 文件写到了哪个位置上
	IoManager fio.IOManager // 之前实现的IO接口, 需要通过它来进行读写管理
}

const DataFileNameSuffix = ".data"

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirpath string, fileId uint32) (*DataFile, error) {
	return nil, nil
	// 待实现
}

// Sync 把数据文件持久化到磁盘中
func (df *DataFile) Sync() error {
	return nil
	// 待实现
}

func (df *DataFile) Write(buf []byte) error {
	return nil
	// 待实现
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
	// TODO
}
