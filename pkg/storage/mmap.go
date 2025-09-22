package storage

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// AccessMode defines file access modes
type AccessMode int

const (
	AccessReadOnly AccessMode = iota
	AccessReadWrite
)

// MapBase provides the base functionality for memory mapping
type MapBase struct {
	addr           unsafe.Pointer
	size           int64
	reservationSize int64
	fd             int
	offset         int64
	accessMode     AccessMode
}

// FileMapper provides memory mapping functionality for database files
type FileMapper struct {
	file   *os.File
	data   []byte
	size   int64
	header *FileHeader
	mapBase MapBase
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

// Map maps the file into memory with specified access mode and size
func (fm *FileMapper) Map() error {
	return fm.MapWithMode(AccessReadWrite, fm.size, 0)
}

// MapWithMode maps the file with specific access mode, size and offset
func (fm *FileMapper) MapWithMode(accessMode AccessMode, size int64, offset int64) error {
	if size == 0 {
		return fmt.Errorf("cannot map empty region")
	}

	var prot int
	switch accessMode {
	case AccessReadOnly:
		prot = syscall.PROT_READ
	case AccessReadWrite:
		prot = syscall.PROT_READ | syscall.PROT_WRITE
	default:
		return fmt.Errorf("invalid access mode")
	}

	data, err := syscall.Mmap(int(fm.file.Fd()), offset, int(size), prot, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}

	fm.data = data
	fm.mapBase.addr = unsafe.Pointer(&data[0])
	fm.mapBase.size = size
	fm.mapBase.reservationSize = size
	fm.mapBase.fd = int(fm.file.Fd())
	fm.mapBase.offset = offset
	fm.mapBase.accessMode = accessMode

	// Initialize header if file is large enough
	if size >= int64(unsafe.Sizeof(FileHeader{})) {
		fm.header = (*FileHeader)(unsafe.Pointer(&data[0]))
	}

	return nil
}

// TryReserve attempts to reserve virtual address space for future mapping
func (fm *FileMapper) TryReserve(size int64, offset int64) bool {
	// Reserve virtual address space using anonymous mapping
	addr, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_NONE, 
		syscall.MAP_PRIVATE|0x1000) // MAP_ANONYMOUS equivalent
	if err != nil {
		return false
	}

	fm.mapBase.addr = unsafe.Pointer(&addr[0])
	fm.mapBase.reservationSize = size
	fm.mapBase.offset = offset
	return true
}

// TryExtendTo attempts to extend the mapping to the specified size
func (fm *FileMapper) TryExtendTo(newSize int64) bool {
	if fm.mapBase.addr == nil || newSize <= fm.mapBase.size {
		return false
	}

	extensionSize := newSize - fm.mapBase.size

	var prot int
	switch fm.mapBase.accessMode {
	case AccessReadOnly:
		prot = syscall.PROT_READ
	case AccessReadWrite:
		prot = syscall.PROT_READ | syscall.PROT_WRITE
	}

	// Try to map the extension
	_, err := syscall.Mmap(fm.mapBase.fd, fm.mapBase.offset+fm.mapBase.size, 
		int(extensionSize), prot, syscall.MAP_SHARED|0x10) // MAP_FIXED equivalent
	if err != nil {
		return false
	}

	fm.mapBase.size = newSize
	// Update data slice to reflect new size
	fm.data = (*[1 << 30]byte)(fm.mapBase.addr)[:newSize:newSize]
	return true
}

// Unmap unmaps the file from memory
func (fm *FileMapper) Unmap() error {
	if fm.mapBase.addr == nil {
		return nil
	}

	err := syscall.Munmap((*[1 << 30]byte)(fm.mapBase.addr)[:fm.mapBase.reservationSize:fm.mapBase.reservationSize])
	fm.data = nil
	fm.header = nil
	fm.mapBase.addr = nil
	fm.mapBase.size = 0
	fm.mapBase.reservationSize = 0
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

// Size returns the mapped size
func (fm *FileMapper) Size() int64 {
	return fm.mapBase.size
}

// ReservationSize returns the reserved size
func (fm *FileMapper) ReservationSize() int64 {
	return fm.mapBase.reservationSize
}

// Sync syncs the mapped memory to disk
func (fm *FileMapper) Sync() error {
	if fm.mapBase.addr == nil {
		return nil
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(fm.mapBase.addr),
		uintptr(fm.mapBase.size),
		syscall.MS_ASYNC)
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}

	return nil
}

// Flush flushes changes to disk with validation
func (fm *FileMapper) Flush(skipValidate bool) error {
	if fm.mapBase.addr == nil {
		return nil
	}

	var flags uintptr = syscall.MS_SYNC
	if skipValidate {
		flags = syscall.MS_ASYNC
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(fm.mapBase.addr),
		uintptr(fm.mapBase.size),
		flags)
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
	if fm.mapBase.addr == nil || offset < 0 || offset >= fm.mapBase.size {
		return nil
	}
	return unsafe.Pointer(uintptr(fm.mapBase.addr) + uintptr(offset))
}

// ReadAt reads data at the given offset
func (fm *FileMapper) ReadAt(offset int64, size int) []byte {
	if fm.data == nil || offset < 0 || offset+int64(size) > fm.mapBase.size {
		return nil
	}
	return fm.data[offset : offset+int64(size)]
}

// WriteAt writes data at the given offset
func (fm *FileMapper) WriteAt(offset int64, data []byte) error {
	if fm.mapBase.addr == nil || fm.mapBase.accessMode == AccessReadOnly {
		return fmt.Errorf("cannot write to read-only mapping")
	}
	
	if offset < 0 || offset+int64(len(data)) > fm.mapBase.size {
		return fmt.Errorf("write out of bounds")
	}
	
	copy(fm.data[offset:], data)
	return nil
}
