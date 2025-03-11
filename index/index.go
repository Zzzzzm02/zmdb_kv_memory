package index

import (
	"bytes"
	"github.com/google/btree"
	"kv_memory/data"
)

// Indexer 定义抽象索引接口1, 后续如果想实现其它数据结构索引就实现这个接口就行
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据对应 key 值找到对应索引的位置
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应索引位置信息
	Delete(key []byte) bool
}

// IndexTpye 索引类型枚举
type IndexTpye = int8

const (
	// Btree 索引
	Btree IndexTpye = iota + 1
	ART
)

// NewIndexer  根据类型初始化索引
func NewIndexer(typ IndexTpye) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("Unsupported index type")
	}
}

// Item 自定义Item类型, btree包接口要用到的参数
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 这个比较结果用于确定元素在树中的位置，保证树的有序性
func (ai Item) Less(bi btree.Item) bool {
	// 原生包bytes
	//Compare returns an integer comparing two byte slices lexicographically.
	//The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	//A nil argument is equivalent to an empty slice.
	return bytes.Compare(ai.key, bi.(*Item).key) == -1

}
