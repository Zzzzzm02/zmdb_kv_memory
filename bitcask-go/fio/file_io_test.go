package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIO(t *testing.T) {

	// 查看创建是否成功
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("001.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	// 顺带测试
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
	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	t.Log(string(b2), err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}
func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

}
