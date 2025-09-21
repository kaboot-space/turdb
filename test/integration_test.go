package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/table"
)

// TestBasicTableOperations tests basic table creation, insertion, and data verification
func TestBasicTableOperations(t *testing.T) {
	// Create temporary file for testing
	tempFile := "/tmp/test_basic_operations.turdb"
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

	// Create table specification
	tableSpec := &table.TableSpec{
		Name:    "users",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{
				Name:     "id",
				Type:     keys.TypeInt,
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     keys.TypeString,
				Nullable: false,
			},
			{
				Name:     "email",
				Type:     keys.TypeString,
				Nullable: true,
			},
			{
				Name:     "age",
				Type:     keys.TypeInt,
				Nullable: true,
			},
		},
	}

	// Create table
	userTable, err := table.NewTable("users", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test 1: Check table exists and is open
	if !userTable.IsOpen() {
		t.Error("Table should be open after creation")
	}

	if userTable.GetName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", userTable.GetName())
	}

	// Test 2: Check columns exist
	idCol, exists := userTable.GetColumn("id")
	if !exists {
		t.Error("Column 'id' should exist")
	}

	nameCol, exists := userTable.GetColumn("name")
	if !exists {
		t.Error("Column 'name' should exist")
	}

	_, exists = userTable.GetColumn("email")
	if !exists {
		t.Error("Column 'email' should exist")
	}

	ageCol, exists := userTable.GetColumn("age")
	if !exists {
		t.Error("Column 'age' should exist")
	}

	// Test 3: Set primary key
	err = userTable.SetPrimaryKeyColumn(idCol)
	if err != nil {
		t.Errorf("Failed to set primary key: %v", err)
	}

	if !userTable.HasPrimaryKey() {
		t.Error("Table should have primary key after setting")
	}

	primaryKey := userTable.GetPrimaryKeyColumn()
	if primaryKey != idCol {
		t.Error("Primary key should match the ID column")
	}

	// Test 4: Create objects
	obj1Key, err := userTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object 1: %v", err)
	}

	obj2Key, err := userTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object 2: %v", err)
	}

	// Test 5: Check objects exist
	if !userTable.ObjectExists(obj1Key) {
		t.Error("Object 1 should exist")
	}

	if !userTable.ObjectExists(obj2Key) {
		t.Error("Object 2 should exist")
	}

	// Test 6: Set values for object 1
	err = userTable.SetValue(obj1Key, "id", int64(1))
	if err != nil {
		t.Errorf("Failed to set id for object 1: %v", err)
	}

	err = userTable.SetValue(obj1Key, "name", "John Doe")
	if err != nil {
		t.Errorf("Failed to set name for object 1: %v", err)
	}

	err = userTable.SetValue(obj1Key, "email", "john@example.com")
	if err != nil {
		t.Errorf("Failed to set email for object 1: %v", err)
	}

	err = userTable.SetValue(obj1Key, "age", int64(30))
	if err != nil {
		t.Errorf("Failed to set age for object 1: %v", err)
	}

	// Test 7: Set values for object 2
	err = userTable.SetValue(obj2Key, "id", int64(2))
	if err != nil {
		t.Errorf("Failed to set id for object 2: %v", err)
	}

	err = userTable.SetValue(obj2Key, "name", "Jane Smith")
	if err != nil {
		t.Errorf("Failed to set name for object 2: %v", err)
	}

	// Leave email as null for object 2
	err = userTable.SetValue(obj2Key, "age", int64(25))
	if err != nil {
		t.Errorf("Failed to set age for object 2: %v", err)
	}

	// Test 8: Retrieve and verify data
	id1, err := userTable.GetValue(obj1Key, "id")
	if err != nil {
		t.Errorf("Failed to get id for object 1: %v", err)
	}
	if id1 != int64(1) {
		t.Errorf("Expected id 1, got %v", id1)
	}

	name1, err := userTable.GetValue(obj1Key, "name")
	if err != nil {
		t.Errorf("Failed to get name for object 1: %v", err)
	}
	if name1 != "John Doe" {
		t.Errorf("Expected name 'John Doe', got %v", name1)
	}

	email1, err := userTable.GetValue(obj1Key, "email")
	if err != nil {
		t.Errorf("Failed to get email for object 1: %v", err)
	}
	if email1 != "john@example.com" {
		t.Errorf("Expected email 'john@example.com', got %v", email1)
	}

	age1, err := userTable.GetValue(obj1Key, "age")
	if err != nil {
		t.Errorf("Failed to get age for object 1: %v", err)
	}
	if age1 != int64(30) {
		t.Errorf("Expected age 30, got %v", age1)
	}

	// Test 9: Verify object 2 data
	name2, err := userTable.GetValue(obj2Key, "name")
	if err != nil {
		t.Errorf("Failed to get name for object 2: %v", err)
	}
	if name2 != "Jane Smith" {
		t.Errorf("Expected name 'Jane Smith', got %v", name2)
	}

	email2, err := userTable.GetValue(obj2Key, "email")
	if err != nil {
		t.Errorf("Failed to get email for object 2: %v", err)
	}
	if email2 != nil {
		t.Errorf("Expected email to be nil for object 2, got %v", email2)
	}

	// Test 10: Check object count
	count := userTable.GetObjectCount()
	if count != 2 {
		t.Errorf("Expected object count 2, got %d", count)
	}

	// Test 11: Use Object interface
	obj1, err := userTable.GetObject(obj1Key)
	if err != nil {
		t.Errorf("Failed to get object 1: %v", err)
	}

	objAge, err := obj1.GetInt(ageCol)
	if err != nil {
		t.Errorf("Failed to get age using Object interface: %v", err)
	}
	if objAge != 30 {
		t.Errorf("Expected age 30 via Object interface, got %d", objAge)
	}

	objName, err := obj1.GetString(nameCol)
	if err != nil {
		t.Errorf("Failed to get name using Object interface: %v", err)
	}
	if objName != "John Doe" {
		t.Errorf("Expected name 'John Doe' via Object interface, got %s", objName)
	}

	t.Logf("✅ Basic table operations test passed!")
}

// TestAllDataTypes tests all supported data types
func TestAllDataTypes(t *testing.T) {
	tempFile := "/tmp/test_data_types.turdb"
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

	// Create table with all data types
	tableSpec := &table.TableSpec{
		Name:    "all_types",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "int_col", Type: keys.TypeInt, Nullable: false},
			{Name: "bool_col", Type: keys.TypeBool, Nullable: false},
			{Name: "string_col", Type: keys.TypeString, Nullable: false},
			{Name: "float_col", Type: keys.TypeFloat, Nullable: false},
			{Name: "double_col", Type: keys.TypeDouble, Nullable: false},
			// Note: Some types like Binary, Mixed, Timestamp, Decimal, UUID will be tested when fully implemented
		},
	}

	typesTable, err := table.NewTable("all_types", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create types table: %v", err)
	}

	// Create object for testing
	objKey, err := typesTable.CreateObject()
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	// Test TypeInt
	err = typesTable.SetValue(objKey, "int_col", int64(42))
	if err != nil {
		t.Errorf("Failed to set int value: %v", err)
	}

	intVal, err := typesTable.GetValue(objKey, "int_col")
	if err != nil {
		t.Errorf("Failed to get int value: %v", err)
	}
	if intVal != int64(42) {
		t.Errorf("Expected int 42, got %v", intVal)
	}

	// Test TypeBool
	err = typesTable.SetValue(objKey, "bool_col", true)
	if err != nil {
		t.Errorf("Failed to set bool value: %v", err)
	}

	boolVal, err := typesTable.GetValue(objKey, "bool_col")
	if err != nil {
		t.Errorf("Failed to get bool value: %v", err)
	}
	if boolVal != true {
		t.Errorf("Expected bool true, got %v", boolVal)
	}

	// Test TypeString
	err = typesTable.SetValue(objKey, "string_col", "Hello World")
	if err != nil {
		t.Errorf("Failed to set string value: %v", err)
	}

	stringVal, err := typesTable.GetValue(objKey, "string_col")
	if err != nil {
		t.Errorf("Failed to get string value: %v", err)
	}
	if stringVal != "Hello World" {
		t.Errorf("Expected string 'Hello World', got %v", stringVal)
	}

	// Test TypeFloat
	err = typesTable.SetValue(objKey, "float_col", float32(3.14))
	if err != nil {
		t.Errorf("Failed to set float value: %v", err)
	}

	floatVal, err := typesTable.GetValue(objKey, "float_col")
	if err != nil {
		t.Errorf("Failed to get float value: %v", err)
	}
	if floatVal != float32(3.14) {
		t.Errorf("Expected float 3.14, got %v", floatVal)
	}

	// Test TypeDouble
	err = typesTable.SetValue(objKey, "double_col", float64(2.718281828))
	if err != nil {
		t.Errorf("Failed to set double value: %v", err)
	}

	doubleVal, err := typesTable.GetValue(objKey, "double_col")
	if err != nil {
		t.Errorf("Failed to get double value: %v", err)
	}
	if doubleVal != float64(2.718281828) {
		t.Errorf("Expected double 2.718281828, got %v", doubleVal)
	}

	t.Logf("✅ All data types test passed!")
}
