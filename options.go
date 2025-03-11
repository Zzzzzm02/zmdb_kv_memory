package kv_memory

// Options 用户的可配置项
type Options struct {
	DirPath      string      // 数据库数据目录
	DataFileSize int64       // 数据文件的大小
	SyncWrite    bool        // 每次写数据是否持久化
	IndexType    IndexerType // 索引数据结构类型
}
type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree 自适应基数树索引
	ART

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)
