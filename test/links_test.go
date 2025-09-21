package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/table"
)

// TestLinkTypes tests Link and TypedLink column types
func TestLinkTypes(t *testing.T) {
	tempFile := "/tmp/test_links.turdb"
	defer os.Remove(tempFile)

	// Initialize storage components
	fileFormat, err := storage.NewFileFormat(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}
	defer fileFormat.Close()

	// Initialize the file format first
	err = fileFormat.Initialize(4096) // 4KB page size
	if err != nil {
		t.Fatalf("Failed to initialize file format: %v", err)
	}

	// Map the file to memory after initialization
	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open/map file format: %v", err)
	}

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Create source table with link columns
	sourceSpec := &table.TableSpec{
		Name:    "sources",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "link_col", Type: keys.TypeLink, Nullable: true},
			{Name: "typed_link_col", Type: keys.TypeTypedLink, Nullable: true},
		},
	}

	sourceTable, err := table.NewTable("sources", sourceSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Create target table
	targetSpec := &table.TableSpec{
		Name:    "targets",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: false},
		},
	}

	targetTable, err := table.NewTable("targets", targetSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create target table: %v", err)
	}

	// Create objects in target table
	targetObj1, err := targetTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create target object 1: %v", err)
	}

	targetObj2, err := targetTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create target object 2: %v", err)
	}

	// Set values in target objects
	err = targetTable.SetValue(targetObj1, "id", int64(100))
	if err != nil {
		t.Errorf("Failed to set target object 1 id: %v", err)
	}

	err = targetTable.SetValue(targetObj1, "name", "Target One")
	if err != nil {
		t.Errorf("Failed to set target object 1 name: %v", err)
	}

	err = targetTable.SetValue(targetObj2, "id", int64(200))
	if err != nil {
		t.Errorf("Failed to set target object 2 id: %v", err)
	}

	err = targetTable.SetValue(targetObj2, "name", "Target Two")
	if err != nil {
		t.Errorf("Failed to set target object 2 name: %v", err)
	}

	// Create source object
	sourceObj, err := sourceTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create source object: %v", err)
	}

	err = sourceTable.SetValue(sourceObj, "id", int64(1))
	if err != nil {
		t.Errorf("Failed to set source object id: %v", err)
	}

	// Test 1: Set Link value (currently testing with ObjKey directly)
	// Note: In a full implementation, this would use an ObjLink type
	err = sourceTable.SetValue(sourceObj, "link_col", targetObj1)
	if err != nil {
		t.Errorf("Failed to set link value: %v", err)
	}

	// Test 2: Get Link value
	linkVal, err := sourceTable.GetValue(sourceObj, "link_col")
	if err != nil {
		t.Errorf("Failed to get link value: %v", err)
	}

	if linkVal != targetObj1 {
		t.Errorf("Expected link value %v, got %v", targetObj1, linkVal)
	}

	// Test 3: Create TypedLink
	typedLink := keys.NewTypedLink(targetTable.GetKey(), targetObj2, keys.TypeString)

	// Test TypedLink value
	err = sourceTable.SetValue(sourceObj, "typed_link_col", typedLink)
	if err != nil {
		t.Errorf("Failed to set typed link value: %v", err)
	}

	// Test 4: Get TypedLink value
	typedLinkVal, err := sourceTable.GetValue(sourceObj, "typed_link_col")
	if err != nil {
		t.Errorf("Failed to get typed link value: %v", err)
	}

	retrievedTypedLink, ok := typedLinkVal.(keys.TypedLink)
	if !ok {
		t.Errorf("Expected TypedLink, got %T", typedLinkVal)
	} else {
		if retrievedTypedLink.TableKey != targetTable.GetKey() {
			t.Errorf("Expected table key %v, got %v", targetTable.GetKey(), retrievedTypedLink.TableKey)
		}
		if retrievedTypedLink.ObjKey != targetObj2 {
			t.Errorf("Expected object key %v, got %v", targetObj2, retrievedTypedLink.ObjKey)
		}
		if retrievedTypedLink.TypeInfo != keys.TypeString {
			t.Errorf("Expected type info %v, got %v", keys.TypeString, retrievedTypedLink.TypeInfo)
		}
	}

	t.Logf("✅ Link types test passed!")
}

// TestValidation tests various validation scenarios
func TestValidation(t *testing.T) {
	tempFile := "/tmp/test_validation.turdb"
	defer os.Remove(tempFile)

	// Initialize storage components
	fileFormat, err := storage.NewFileFormat(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}
	defer fileFormat.Close()

	// Initialize the file format first
	err = fileFormat.Initialize(4096) // 4KB page size
	if err != nil {
		t.Fatalf("Failed to initialize file format: %v", err)
	}

	// Map the file to memory after initialization
	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open/map file format: %v", err)
	}

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Test 1: Validate column name length
	longName := make([]byte, 65) // Max is 63
	for i := range longName {
		longName[i] = 'a'
	}

	invalidSpec := &table.TableSpec{
		Name:    "validation_test",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: string(longName), Type: keys.TypeString, Nullable: false},
		},
	}

	_, err = table.NewTable("validation_test", invalidSpec, group, alloc, fileFormat)
	if err == nil {
		t.Error("Should fail to create table with column name longer than 63 characters")
	}

	// Test 2: Valid table creation
	validSpec := &table.TableSpec{
		Name:    "validation_test",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: true},
		},
	}

	validTable, err := table.NewTable("validation_test", validSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create valid table: %v", err)
	}

	// Test 3: Validate null values on non-nullable columns
	objKey, err := validTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	// Should fail to set null on non-nullable column
	err = validTable.SetValue(objKey, "id", nil)
	if err == nil {
		t.Error("Should fail to set null value on non-nullable column")
	}

	// Should succeed to set null on nullable column
	err = validTable.SetValue(objKey, "name", nil)
	if err != nil {
		t.Errorf("Should succeed to set null value on nullable column: %v", err)
	}

	// Test 4: Type validation
	err = validTable.SetValue(objKey, "id", "not an integer")
	if err == nil {
		t.Error("Should fail to set string value on integer column")
	}

	err = validTable.SetValue(objKey, "name", 123)
	if err == nil {
		t.Error("Should fail to set integer value on string column")
	}

	// Test 5: Duplicate column names
	_, err = validTable.AddColumn("name", keys.TypeInt, false)
	if err == nil {
		t.Error("Should fail to add column with duplicate name")
	}

	// Test 6: Primary key validation
	nameCol, _ := validTable.GetColumn("name")
	err = validTable.SetPrimaryKeyColumn(nameCol)
	if err == nil {
		t.Error("Should fail to set nullable column as primary key")
	}

	idCol, _ := validTable.GetColumn("id")
	err = validTable.SetPrimaryKeyColumn(idCol)
	if err != nil {
		t.Errorf("Should succeed to set non-nullable column as primary key: %v", err)
	}

	t.Logf("✅ Validation test passed!")
}

// TestSchemaOperations tests schema hash and modification tracking
func TestSchemaOperations(t *testing.T) {
	tempFile := "/tmp/test_schema.turdb"
	defer os.Remove(tempFile)

	// Initialize storage components
	fileFormat, err := storage.NewFileFormat(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}
	defer fileFormat.Close()

	// Initialize the file format first
	err = fileFormat.Initialize(4096) // 4KB page size
	if err != nil {
		t.Fatalf("Failed to initialize file format: %v", err)
	}

	// Map the file to memory after initialization
	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open/map file format: %v", err)
	}

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Create table
	tableSpec := &table.TableSpec{
		Name:    "schema_test",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
		},
	}

	schemaTable, err := table.NewTable("schema_test", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create schema table: %v", err)
	}

	// Test 1: Get initial schema
	spec := schemaTable.GetSpec()
	if spec.Name != "schema_test" {
		t.Errorf("Expected table name 'schema_test', got '%s'", spec.Name)
	}

	if spec.Version != 1 {
		t.Errorf("Expected version 1, got %d", spec.Version)
	}

	initialModified := spec.Modified

	// Test 2: Update schema hash
	schemaTable.UpdateSchemaHash()

	updatedSpec := schemaTable.GetSpec()
	if updatedSpec.Modified == initialModified {
		t.Error("Modified timestamp should be updated after schema hash update")
	}

	// Test 3: Add column and verify schema changes
	_, err = schemaTable.AddColumn("name", keys.TypeString, false)
	if err != nil {
		t.Errorf("Failed to add column: %v", err)
	}

	finalSpec := schemaTable.GetSpec()
	if finalSpec.Modified == initialModified {
		t.Error("Modified timestamp should be updated after adding column")
	}

	// Test 4: Validate schema
	err = schemaTable.ValidateSchema()
	if err != nil {
		t.Errorf("Schema validation should pass: %v", err)
	}

	t.Logf("✅ Schema operations test passed!")
}
