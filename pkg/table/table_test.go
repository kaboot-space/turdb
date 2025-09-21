package table

import (
	"path/filepath"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// createTestFileFormat creates a properly initialized FileFormat for testing
func createTestFileFormat(t *testing.T) *storage.FileFormat {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.tur")

	fileFormat, err := storage.NewFileFormat(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}

	err = fileFormat.Initialize(4096)
	if err != nil {
		t.Fatalf("Failed to initialize file: %v", err)
	}

	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	return fileFormat
}

func TestNewTable(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	spec := &TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: true},
		},
	}

	table, err := NewTable("test_table", spec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if table.spec.Name != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", table.spec.Name)
	}

	if len(table.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.columns))
	}

	if !table.isOpen {
		t.Error("Expected table to be open")
	}
}

func TestTableAddColumn(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	spec := &TableSpec{
		Name:    "test_table",
		Columns: []ColumnSpec{},
	}

	table, err := NewTable("test_table", spec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	colKey, err := table.AddColumn("test_col", keys.TypeString, true)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}

	if colKey == 0 {
		t.Error("Expected non-zero column key")
	}

	if len(table.columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(table.columns))
	}

	// Test adding duplicate column
	_, err = table.AddColumn("test_col", keys.TypeString, true)
	if err == nil {
		t.Error("Expected error when adding duplicate column")
	}
}

func TestTableCreateObject(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	spec := &TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
		},
	}

	table, err := NewTable("test_table", spec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	objKey, err := table.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	if objKey == 0 {
		t.Error("Expected non-zero object key")
	}
}

func TestTableGetObjectCount(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	spec := &TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
		},
	}

	table, err := NewTable("test_table", spec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Initially should have 0 objects
	count := table.GetObjectCount()
	if count != 0 {
		t.Errorf("Expected 0 objects, got %d", count)
	}

	// Create an object
	_, err = table.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	count = table.GetObjectCount()
	if count != 1 {
		t.Errorf("Expected 1 object, got %d", count)
	}
}

func TestTableClose(t *testing.T) {
	fileFormat := createTestFileFormat(t)
	defer fileFormat.Close()

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	spec := &TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
		},
	}

	table, err := NewTable("test_table", spec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	table.Close()

	if table.isOpen {
		t.Error("Table should be closed")
	}

	// Test operations on closed table
	_, err = table.CreateObject()
	if err == nil {
		t.Error("Should not be able to create object on closed table")
	}
}
