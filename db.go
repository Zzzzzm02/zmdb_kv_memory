package kv_memory

import (
	"errors"
	data2 "kv_memory/data"
	"kv_memory/index"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	options    Options                    // 用户配置选项
	mu         *sync.RWMutex              // 锁
	activeFile *data2.DataFile            // 当前活跃数据文件, 可以用于写入
	olderFiles map[uint32]*data2.DataFile // 旧的数据文件, 只能用于读
	index      index.Indexer              // 内存索引
}

// open 打开 bitcask 存储引擎实例
func open(opt Options) (*DB, error) {
	// 首先调用函数检查用户配置项
	if err := checkOptions(opt); err != nil {
		return nil, err
	}

	// 然后是加载数据文件

}

// Put 这个方法写入 key/value 数据, key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data2.LogRecord{
		Key:   key,
		Value: value,
		Type:  data2.LogRecordNormal,
	}

	// appendLogRecord 方法会返回一个索引位置 *data.LogRecordPos
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引, 失败则返回错误
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data2.LogRecord) (*data2.LogRecordPos, error) {
	// 先加锁
	db.mu.Lock()
	defer db.mu.Unlock()
	// 判断当前活跃文件是否存在, 因为数据在没有写入的时候是没有文件生成的
	// 如果为空, 则初始化文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data2.EncodeLogRecord(logRecord)

	// 如果写入的数据已经到达了活跃文件的阈值, 则关闭活跃文件, 并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// activeFile 里面的数据先要持久化到磁盘中, 保证已有数据的持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造一个内存索引的信息, 并返回
	pos := &data2.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		// 这里的意思是, 判断到活跃文件不为空, 新的活跃文件 id 就是
		// 当前文件 id + 1
		// 为啥
		initialFileId = db.activeFile.FileId + 1
	}

	// 每个数据文件在创建的时候, Id 都是递增的

	// 打开新的数据文件, 需要传一个目录
	dataFile, err := data2.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// checkOptions 检查用户配置项是否有错，无错返回 nil
func checkOptions(opt Options) error {

	// 检查用户传进来的数据库目录是否为空
	if opt.DirPath == "" {
		return errors.New("database DirPath is empty")
	}

	// 检查文件大小数值合法
	if opt.DataFileSize <= 0 {
		return errors.New("database DataFileSize must be greeter than 0")
	}

	return nil
}
