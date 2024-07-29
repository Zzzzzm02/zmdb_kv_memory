package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
	// 通过defer去调用本函数, 延迟删除临时资源
}
func TestNewFileIO(t *testing.T) {

	// 查看创建是否成功
	path := filepath.Join("001.data")
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	// 最后还得顺手把他关了才能删除
	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {

	path := filepath.Join("001.data")
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	// 顺带测试 会追加写的
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte(" "))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	// 第二个测试用例
	// 注意加了个空格, 那偏移量就要变一下了...
	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 6)
	t.Log(string(b2), err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Write(t *testing.T) {

	path := filepath.Join("001.data")
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("001.data")
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	// 查看有没有出错
	err = fio.Sync()
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}
func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("001.data")
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	// 正常关闭
	err = fio.Close()
	assert.Nil(t, err)
}
