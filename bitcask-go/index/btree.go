package index

import (
	"github.com/google/btree"
	"kv_memory/bitcask-go/data"
	"sync"
)

// BTree 主要去封装引用的google的b树结构
// import "github.com/google/btree"
type BTree struct {
	// 一个是google包里面的b树
	tree *btree.BTree
	lock *sync.RWMutex
	// 并发写操作需要加锁机制
}

// NewBTree 初始化BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 实现
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{pos: pos, key: key}
	// 加锁机制
	bt.lock.Lock()
	// 调用tree这个包的函数
	bt.tree.ReplaceOrInsert(it)

	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)

	if btreeItem == nil {
		return nil
	}
	// 进行简单转换
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Get(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
