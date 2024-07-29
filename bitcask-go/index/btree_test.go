package index

import (
	"github.com/stretchr/testify/assert"
	"kv_memory/bitcask-go/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	// key 为nil时的边界值
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 101})
	// testify库的函数
	//这里 res 是 bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 101}) 的返回值
	//表示插入操作是否成功
	//如果 res 不为 true，则测试会失败，并显示相关错误信息
	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 102})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	// key 为nil时的边界值
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 101})
	assert.True(t, res)

	pos1 := bt.Get(nil)
	t.Log(pos1) // &{1 101}
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 102})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 102}) // 重复更新
	assert.True(t, res3)
	pos2 := bt.Get([]byte("a")) // 都要转成byte切片呀

	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(102), pos2.Offset) // 应该更新成102才对

}
func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	// key 为nil时的边界值
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 101})
	assert.True(t, res)

	resDelete := bt.Delete(nil)
	assert.True(t, resDelete)
	resDelete2 := bt.Delete(nil)
	assert.True(t, resDelete2)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 102})
	assert.True(t, res2)
	resDelete3 := bt.Delete([]byte("a"))
	assert.True(t, resDelete3)
	resDelete4 := bt.Delete([]byte("a")) // 可以重复删除的嘛
	assert.True(t, resDelete4)
}
