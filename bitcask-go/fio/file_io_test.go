package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIO(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("tem", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

}
func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tem", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

}
