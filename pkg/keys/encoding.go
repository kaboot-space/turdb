package keys

import (
	"encoding/binary"
	"fmt"
)

// KeyEncoder provides utilities for encoding and decoding keys
type KeyEncoder struct{}

// NewKeyEncoder creates a new key encoder
func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{}
}

// EncodeTableKey encodes a TableKey to bytes
func (ke *KeyEncoder) EncodeTableKey(key TableKey) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(key))
	return buf
}

// DecodeTableKey decodes bytes to a TableKey
func (ke *KeyEncoder) DecodeTableKey(data []byte) (TableKey, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for TableKey")
	}
	return TableKey(binary.LittleEndian.Uint32(data)), nil
}

// EncodeColKey encodes a ColKey to bytes
func (ke *KeyEncoder) EncodeColKey(key ColKey) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(key))
	return buf
}

// DecodeColKey decodes bytes to a ColKey
func (ke *KeyEncoder) DecodeColKey(data []byte) (ColKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ColKey")
	}
	return ColKey(binary.LittleEndian.Uint64(data)), nil
}

// EncodeObjKey encodes an ObjKey to bytes
func (ke *KeyEncoder) EncodeObjKey(key ObjKey) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(key))
	return buf
}

// DecodeObjKey decodes bytes to an ObjKey
func (ke *KeyEncoder) DecodeObjKey(data []byte) (ObjKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ObjKey")
	}
	return ObjKey(binary.LittleEndian.Uint64(data)), nil
}

// KeySize constants
const (
	TableKeySize = 4
	ColKeySize   = 8
	ObjKeySize   = 8
)

// GetKeySize returns the size in bytes for a given key type
func GetKeySize(keyType string) int {
	switch keyType {
	case "table":
		return TableKeySize
	case "column":
		return ColKeySize
	case "object":
		return ObjKeySize
	default:
		return 0
	}
}

// KeyBatch represents a batch of keys for efficient encoding/decoding
type KeyBatch struct {
	TableKeys []TableKey
	ColKeys   []ColKey
	ObjKeys   []ObjKey
}

// NewKeyBatch creates a new key batch
func NewKeyBatch() *KeyBatch {
	return &KeyBatch{
		TableKeys: make([]TableKey, 0),
		ColKeys:   make([]ColKey, 0),
		ObjKeys:   make([]ObjKey, 0),
	}
}

// AddTableKey adds a table key to the batch
func (kb *KeyBatch) AddTableKey(key TableKey) {
	kb.TableKeys = append(kb.TableKeys, key)
}

// AddColKey adds a column key to the batch
func (kb *KeyBatch) AddColKey(key ColKey) {
	kb.ColKeys = append(kb.ColKeys, key)
}

// AddObjKey adds an object key to the batch
func (kb *KeyBatch) AddObjKey(key ObjKey) {
	kb.ObjKeys = append(kb.ObjKeys, key)
}

// Encode encodes the entire batch to bytes
func (kb *KeyBatch) Encode() []byte {
	encoder := NewKeyEncoder()

	// Calculate total size
	totalSize := 12 // 3 * 4 bytes for counts
	totalSize += len(kb.TableKeys) * TableKeySize
	totalSize += len(kb.ColKeys) * ColKeySize
	totalSize += len(kb.ObjKeys) * ObjKeySize

	buf := make([]byte, totalSize)
	offset := 0

	// Write counts
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(kb.TableKeys)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(kb.ColKeys)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(kb.ObjKeys)))
	offset += 4

	// Write table keys
	for _, key := range kb.TableKeys {
		keyBytes := encoder.EncodeTableKey(key)
		copy(buf[offset:], keyBytes)
		offset += TableKeySize
	}

	// Write column keys
	for _, key := range kb.ColKeys {
		keyBytes := encoder.EncodeColKey(key)
		copy(buf[offset:], keyBytes)
		offset += ColKeySize
	}

	// Write object keys
	for _, key := range kb.ObjKeys {
		keyBytes := encoder.EncodeObjKey(key)
		copy(buf[offset:], keyBytes)
		offset += ObjKeySize
	}

	return buf
}

// Decode decodes bytes to a key batch
func (kb *KeyBatch) Decode(data []byte) error {
	if len(data) < 12 {
		return fmt.Errorf("insufficient data for key batch header")
	}

	encoder := NewKeyEncoder()
	offset := 0

	// Read counts
	tableCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	colCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	objCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Initialize slices
	kb.TableKeys = make([]TableKey, tableCount)
	kb.ColKeys = make([]ColKey, colCount)
	kb.ObjKeys = make([]ObjKey, objCount)

	// Read table keys
	for i := uint32(0); i < tableCount; i++ {
		if offset+TableKeySize > len(data) {
			return fmt.Errorf("insufficient data for table key %d", i)
		}
		key, err := encoder.DecodeTableKey(data[offset:])
		if err != nil {
			return err
		}
		kb.TableKeys[i] = key
		offset += TableKeySize
	}

	// Read column keys
	for i := uint32(0); i < colCount; i++ {
		if offset+ColKeySize > len(data) {
			return fmt.Errorf("insufficient data for column key %d", i)
		}
		key, err := encoder.DecodeColKey(data[offset:])
		if err != nil {
			return err
		}
		kb.ColKeys[i] = key
		offset += ColKeySize
	}

	// Read object keys
	for i := uint32(0); i < objCount; i++ {
		if offset+ObjKeySize > len(data) {
			return fmt.Errorf("insufficient data for object key %d", i)
		}
		key, err := encoder.DecodeObjKey(data[offset:])
		if err != nil {
			return err
		}
		kb.ObjKeys[i] = key
		offset += ObjKeySize
	}

	return nil
}

// Clear clears all keys from the batch
func (kb *KeyBatch) Clear() {
	kb.TableKeys = kb.TableKeys[:0]
	kb.ColKeys = kb.ColKeys[:0]
	kb.ObjKeys = kb.ObjKeys[:0]
}

// Size returns the total number of keys in the batch
func (kb *KeyBatch) Size() int {
	return len(kb.TableKeys) + len(kb.ColKeys) + len(kb.ObjKeys)
}
