package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecordPos 定义索引数据结构
type LogRecordPos struct {
	Fid    uint32 // 文件id, 表示存到了哪个文件夹里边
	Offset int64  // 偏移量, 表示存到了数据文件的哪个位置
}

// LogRecord 写入到数据文件中的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，实现了类似于日志的形式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}
