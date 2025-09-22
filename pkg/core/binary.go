package core

import (
	"encoding/binary"
)

// WriteUint32 writes a uint32 value to the byte slice at the given offset
func WriteUint32(data []byte, value uint32) {
	binary.LittleEndian.PutUint32(data, value)
}

// ReadUint32 reads a uint32 value from the byte slice at the given offset
func ReadUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

// WriteUint64 writes a uint64 value to the byte slice at the given offset
func WriteUint64(data []byte, value uint64) {
	binary.LittleEndian.PutUint64(data, value)
}

// ReadUint64 reads a uint64 value from the byte slice at the given offset
func ReadUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// WriteUint16 writes a uint16 value to the byte slice at the given offset
func WriteUint16(data []byte, value uint16) {
	binary.LittleEndian.PutUint16(data, value)
}

// ReadUint16 reads a uint16 value from the byte slice at the given offset
func ReadUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

// WriteUint8 writes a uint8 value to the byte slice at the given offset
func WriteUint8(data []byte, value uint8) {
	data[0] = value
}

// ReadUint8 reads a uint8 value from the byte slice at the given offset
func ReadUint8(data []byte) uint8 {
	return data[0]
}