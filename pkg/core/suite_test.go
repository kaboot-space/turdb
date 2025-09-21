package core

import (
	"path/filepath"
	"testing"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// createTestFileFormat creates a properly initialized FileFormat for testing
func createTestFileFormat(t *testing.T) *storage.FileFormat {
	// Create temporary file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.tur")

	fileFormat, err := storage.NewFileFormat(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}

	// Initialize the file
	err = fileFormat.Initialize(4096) // 4KB page size
	if err != nil {
		t.Fatalf("Failed to initialize file: %v", err)
	}

	// Open the file for use
	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	return fileFormat
}

// TestAllocator tests the enhanced allocator functionality
func TestAllocator(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	allocator := NewAllocator(fileFormat)

	// Test basic allocation
	ref1, err := allocator.Alloc(64)
	if err != nil {
		t.Fatalf("Failed to allocate: %v", err)
	}
	if ref1 == 0 {
		t.Fatal("Expected non-zero reference")
	}

	// Test alignment
	ref2, err := allocator.Alloc(33) // Should be aligned to 8 bytes
	if err != nil {
		t.Fatalf("Failed to allocate aligned: %v", err)
	}

	// Test free and reallocation
	allocator.Free(ref1, 64)
	ref3, err := allocator.Alloc(32) // Should reuse freed space
	if err != nil {
		t.Fatalf("Failed to reallocate: %v", err)
	}

	// Test coalescing by freeing adjacent blocks
	allocator.Free(ref2, 40) // Aligned size
	allocator.Free(ref3, 32)

	// Allocate larger block that should use coalesced space
	ref4, err := allocator.Alloc(64)
	if err != nil {
		t.Fatalf("Failed to allocate after coalescing: %v", err)
	}

	allocator.Free(ref4, 64)
}

// TestSlabAlloc tests the slab allocator
func TestSlabAlloc(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	allocator := NewAllocator(fileFormat)
	slabAlloc := NewSlabAlloc(allocator)

	// Test slab allocation
	ref1, err := slabAlloc.Alloc(32)
	if err != nil {
		t.Fatalf("Failed to allocate from slab: %v", err)
	}
	if ref1 == 0 {
		t.Fatal("Expected non-zero slab reference")
	}

	ref2, err := slabAlloc.Alloc(32)
	if err != nil {
		t.Fatalf("Failed to allocate from slab: %v", err)
	}
	if ref2 == 0 {
		t.Fatal("Expected non-zero slab reference")
	}

	// Test slab free
	slabAlloc.Free(ref1, 32)
	slabAlloc.Free(ref2, 32)

	// Test reallocation after free
	ref3, err := slabAlloc.Alloc(32)
	if err != nil {
		t.Fatalf("Failed to reallocate from slab: %v", err)
	}
	if ref3 == 0 {
		t.Fatal("Expected non-zero slab reference after free")
	}

	slabAlloc.Free(ref3, 32)
}

// TestArray tests the enhanced array functionality
func TestArray(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	allocator := NewAllocator(fileFormat)

	// Test array creation
	array := NewArray(keys.TypeString, allocator)
	if array == nil {
		t.Fatal("Failed to create array")
	}

	// Test array creation in file
	err := array.Create(100)
	if err != nil {
		t.Fatalf("Failed to create array in file: %v", err)
	}

	// Test inner array flag
	array.isInner = true
	if !array.isInner {
		t.Fatal("Expected inner array flag to be set")
	}
}

// TestColumnArray tests the column array implementation
func TestColumnArray(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	allocator := NewAllocator(fileFormat)
	colKey := keys.NewColKey(keys.TypeString, 1, false)

	// Test column array creation
	colArray := NewColumnArray(colKey, allocator)
	if colArray == nil {
		t.Fatal("Failed to create column array")
	}

	// Test column key
	if colArray.colKey != colKey {
		t.Fatal("Column key mismatch")
	}

	// Test array creation
	err := colArray.Create(50)
	if err != nil {
		t.Fatalf("Failed to create column array: %v", err)
	}
}

// TestKeys tests the key system functionality
func TestKeys(t *testing.T) {
	// Test TableKey
	tableKey := keys.NewTableKey(42)
	if !tableKey.IsValid() {
		t.Fatal("Expected valid table key")
	}
	if tableKey.GetValue() != 42 {
		t.Fatal("Table key value mismatch")
	}

	// Test ColKey
	colKey := keys.NewColKey(keys.TypeString, 1, true)
	if !colKey.IsNullable() {
		t.Fatal("Expected nullable column key")
	}
	if colKey.GetType() != keys.TypeString {
		t.Fatal("Column key type mismatch")
	}
	if colKey.GetIndex() != 1 {
		t.Fatal("Column key index mismatch")
	}

	// Test backlink functionality
	backlinkKey := colKey.SetBacklinkIndex(100)
	if !backlinkKey.IsBacklink() {
		t.Fatal("Expected backlink column")
	}
	if backlinkKey.GetBacklinkIndex() != 100 {
		t.Fatal("Backlink index mismatch")
	}

	// Test ObjKey
	objKey := keys.NewObjKey(12345)
	if objKey.IsUnresolved() {
		t.Fatal("Expected resolved object key")
	}
	if objKey.GetValue() != 12345 {
		t.Fatal("Object key value mismatch")
	}

	// Test unresolved object key
	unresolvedKey := keys.ObjKey(0)
	if !unresolvedKey.IsUnresolved() {
		t.Fatal("Expected unresolved object key")
	}
}

// TestKeyEncoding tests key encoding and decoding
func TestKeyEncoding(t *testing.T) {
	encoder := keys.NewKeyEncoder()

	// Test TableKey encoding/decoding
	originalTable := keys.NewTableKey(123)
	encodedTable := encoder.EncodeTableKey(originalTable)
	decodedTable, err := encoder.DecodeTableKey(encodedTable)
	if err != nil {
		t.Fatalf("Failed to decode table key: %v", err)
	}
	if decodedTable != originalTable {
		t.Fatal("Table key encoding/decoding mismatch")
	}

	// Test ColKey encoding/decoding
	originalCol := keys.NewColKey(keys.TypeInt, 1, true)
	encodedCol := encoder.EncodeColKey(originalCol)
	decodedCol, err := encoder.DecodeColKey(encodedCol)
	if err != nil {
		t.Fatalf("Failed to decode column key: %v", err)
	}
	if decodedCol != originalCol {
		t.Fatal("Column key encoding/decoding mismatch")
	}

	// Test ObjKey encoding/decoding
	originalObj := keys.NewObjKey(789)
	encodedObj := encoder.EncodeObjKey(originalObj)
	decodedObj, err := encoder.DecodeObjKey(encodedObj)
	if err != nil {
		t.Fatalf("Failed to decode object key: %v", err)
	}
	if decodedObj != originalObj {
		t.Fatal("Object key encoding/decoding mismatch")
	}
}

// TestKeyBatch tests batch key operations
func TestKeyBatch(t *testing.T) {
	batch := keys.NewKeyBatch()

	// Add keys to batch
	batch.AddTableKey(keys.NewTableKey(1))
	batch.AddTableKey(keys.NewTableKey(2))
	batch.AddColKey(keys.NewColKey(keys.TypeString, 1, false))
	batch.AddColKey(keys.NewColKey(keys.TypeInt, 2, true))
	batch.AddObjKey(keys.NewObjKey(100))
	batch.AddObjKey(keys.NewObjKey(200))

	// Test batch size
	if batch.Size() != 6 {
		t.Fatal("Expected batch size of 6")
	}

	// Test batch encoding/decoding
	encoded := batch.Encode()
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded batch")
	}

	decodedBatch := keys.NewKeyBatch()
	err := decodedBatch.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode batch: %v", err)
	}

	// Verify decoded batch
	if len(decodedBatch.TableKeys) != 2 {
		t.Fatal("Expected 2 table keys in decoded batch")
	}
	if len(decodedBatch.ColKeys) != 2 {
		t.Fatal("Expected 2 column keys in decoded batch")
	}
	if len(decodedBatch.ObjKeys) != 2 {
		t.Fatal("Expected 2 object keys in decoded batch")
	}

	// Test clear
	batch.Clear()
	if batch.Size() != 0 {
		t.Fatal("Expected empty batch after clear")
	}
}

// BenchmarkAllocator benchmarks allocator performance
func BenchmarkAllocator(b *testing.B) {
	// Create temporary file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.tur")

	fileFormat, err := storage.NewFileFormat(tmpFile)
	if err != nil {
		b.Fatalf("Failed to create file format: %v", err)
	}
	defer fileFormat.Close()

	err = fileFormat.Initialize(4096)
	if err != nil {
		b.Fatalf("Failed to initialize file: %v", err)
	}

	err = fileFormat.Open()
	if err != nil {
		b.Fatalf("Failed to open file: %v", err)
	}

	allocator := NewAllocator(fileFormat)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref, err := allocator.Alloc(64)
		if err != nil {
			b.Fatal(err)
		}
		allocator.Free(ref, 64)
	}
}

// BenchmarkKeyEncoding benchmarks key encoding performance
func BenchmarkKeyEncoding(b *testing.B) {
	encoder := keys.NewKeyEncoder()
	colKey := keys.NewColKey(keys.TypeString, 123, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded := encoder.EncodeColKey(colKey)
		_, err := encoder.DecodeColKey(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
