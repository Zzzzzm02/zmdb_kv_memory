package data

// LogRecordPos 定义索引数据结构
type LogRecordPos struct {
	Fid    uint32 // 文件id, 表示存到了哪个文件夹里边
	Offset int64  // 偏移量, 表示存到了数据文件的哪个位置
}
