package storage

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// FileMapper provides memory mapping functionality for database files
type FileMapper struct {
	file   *os.File
	data   []byte
	size   int64
	header *FileHeader
}

// NewFileMapper creates a new file mapper for the given file
func NewFileMapper(file *os.File) (*FileMapper, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	return &FileMapper{
		file: file,
		size: stat.Size(),
	}, nil
}

// Map maps the file into memory
func (fm *FileMapper) Map() error {
	if fm.size == 0 {
		return fmt.Errorf("cannot map empty file")
	}

	data, err := syscall.Mmap(int(fm.file.Fd()), 0, int(fm.size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}

	fm.data = data

	// Initialize header if file is large enough
	if fm.size >= int64(unsafe.Sizeof(FileHeader{})) {
		fm.header = (*FileHeader)(unsafe.Pointer(&data[0]))
	}

	return nil
}

// Unmap unmaps the file from memory
func (fm *FileMapper) Unmap() error {
	if fm.data == nil {
		return nil
	}

	err := syscall.Munmap(fm.data)
	fm.data = nil
	fm.header = nil
	return err
}

// GetData returns the mapped data
func (fm *FileMapper) GetData() []byte {
	return fm.data
}

// GetHeader returns the file header
func (fm *FileMapper) GetHeader() *FileHeader {
	return fm.header
}

// Size returns the file size
func (fm *FileMapper) Size() int64 {
	return fm.size
}

// Sync syncs the mapped memory to disk
func (fm *FileMapper) Sync() error {
	if fm.data == nil {
		return nil
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&fm.data[0])),
		uintptr(len(fm.data)),
		syscall.MS_ASYNC)
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}

	return nil
}

// Close closes the file mapper and unmaps memory
func (fm *FileMapper) Close() error {
	if err := fm.Unmap(); err != nil {
		return err
	}
	return fm.file.Close()
}

// GetPointer returns a pointer to data at the given offset
func (fm *FileMapper) GetPointer(offset int64) unsafe.Pointer {
	if fm.data == nil || offset < 0 || offset >= fm.size {
		return nil
	}
	return unsafe.Pointer(&fm.data[offset])
}

// ReadAt reads data at the given offset
func (fm *FileMapper) ReadAt(offset int64, size int) []byte {
	if fm.data == nil || offset < 0 || offset+int64(size) > fm.size {
		return nil
	}
	return fm.data[offset : offset+int64(size)]
}
