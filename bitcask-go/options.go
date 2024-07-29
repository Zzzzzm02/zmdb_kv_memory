package bitcask_go

// Options 用户的可配置项
type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件的大小
	SyncWrite    bool   // 每次写数据是否持久化
}
