package kv_memory

import (
	"errors"
	"io"
	"kv_memory/data"
	"kv_memory/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	options    Options                   // 用户配置选项
	mu         *sync.RWMutex             // 锁
	activeFile *data.DataFile            // 当前活跃数据文件, 可以用于写入
	fileIds    []int                     // 文件 id ， 只能在加载索引的时候使用，不能用于其他地方
	olderFiles map[uint32]*data.DataFile // 旧的数据文件, 只能用于读
	index      index.Indexer             // 内存索引
}

// Open 打开 bitcask 存储引擎实例
func Open(opt Options) (*DB, error) {
	// 首先调用函数检查用户配置项
	if err := checkOptions(opt); err != nil {
		return nil, err
	}

	// 然后是校验目录是否存在，如不存在就创建新目录
	if _, err := os.Stat(opt.DirPath); err == nil {
		if err := os.MkdirAll(opt.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//  初始化db实例结构体
	db := &DB{
		options:    opt,                             // 用户配置选项
		mu:         &sync.RWMutex{},                 // 锁
		olderFiles: make(map[uint32]*data.DataFile), // 旧的数据文件, 只能用于读
		index:      index.NewIndexer(opt.IndexType), // 内存索引
	}
	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 然后是对索引的处理
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	// 全部完成
	return db, nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 如果文件数量长度为零，说明不用创建索引
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有的文件id，处理所有的文件记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)

		var dataFile *data.DataFile
		// 拿到当前活跃的文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {

			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					// 正常结束需跳出循环
					break
				}
				return err
			}
			// 拿到 logRecord 之后，就要构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增 offset 下一次从新的位置开始
			offset += size
		}
		// 如果是当前活跃文件，需要更新 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// Put 这个方法写入 key/value 数据, key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
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
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	encRecord, size := data.EncodeLogRecord(logRecord)

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
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int

	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏了
			if err != nil {
				return ErrDataDirectorycorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对文件ID进行排序，从小到大依次加载
	sort.Ints(fileIds)

	// 排序之后，赋值到实例参数里面
	db.fileIds = fileIds
	// 遍历每个ID，依次打开数据文件
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个，ID是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的数据文件
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}
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
