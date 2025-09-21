package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/table"
)

// TestCollectionTypes tests List, Set, and Dictionary column types
func TestCollectionTypes(t *testing.T) {
	tempFile := "/tmp/test_collections.turdb"
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

	// Create table specification with collection columns
	tableSpec := &table.TableSpec{
		Name:    "collections_test",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{
				Name:     "id",
				Type:     keys.TypeInt,
				Nullable: false,
			},
		},
	}

	collectionsTable, err := table.NewTable("collections_test", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create collections table: %v", err)
	}

	// Test 1: Add List column
	listCol, err := collectionsTable.AddColumnList("string_list", keys.TypeString, true)
	if err != nil {
		t.Errorf("Failed to add list column: %v", err)
	}

	// Verify list column exists
	listColCheck, exists := collectionsTable.GetColumn("string_list")
	if !exists {
		t.Error("List column should exist after creation")
	}
	if listColCheck != listCol {
		t.Error("List column key should match")
	}

	// Test 2: Add Set column
	setCol, err := collectionsTable.AddColumnSet("int_set", keys.TypeInt, false)
	if err != nil {
		t.Errorf("Failed to add set column: %v", err)
	}

	// Verify set column exists
	setColCheck, exists := collectionsTable.GetColumn("int_set")
	if !exists {
		t.Error("Set column should exist after creation")
	}
	if setColCheck != setCol {
		t.Error("Set column key should match")
	}

	// Test 3: Add Dictionary column
	dictCol, err := collectionsTable.AddColumnDictionary("string_dict", keys.TypeString, keys.TypeInt, true)
	if err != nil {
		t.Errorf("Failed to add dictionary column: %v", err)
	}

	// Verify dictionary column exists
	dictColCheck, exists := collectionsTable.GetColumn("string_dict")
	if !exists {
		t.Error("Dictionary column should exist after creation")
	}
	if dictColCheck != dictCol {
		t.Error("Dictionary column key should match")
	}

	// Test 4: Verify collection element types
	listElementType, err := collectionsTable.GetCollectionElementType(listCol)
	if err != nil {
		t.Errorf("Failed to get list element type: %v", err)
	}
	if listElementType != keys.TypeString {
		t.Errorf("Expected list element type String, got %v", listElementType)
	}

	setElementType, err := collectionsTable.GetCollectionElementType(setCol)
	if err != nil {
		t.Errorf("Failed to get set element type: %v", err)
	}
	if setElementType != keys.TypeInt {
		t.Errorf("Expected set element type Int, got %v", setElementType)
	}

	dictElementType, err := collectionsTable.GetCollectionElementType(dictCol)
	if err != nil {
		t.Errorf("Failed to get dictionary element type: %v", err)
	}
	if dictElementType != keys.TypeInt {
		t.Errorf("Expected dictionary element type Int, got %v", dictElementType)
	}

	// Test 5: Verify dictionary key type
	dictKeyType, err := collectionsTable.GetDictionaryKeyType(dictCol)
	if err != nil {
		t.Errorf("Failed to get dictionary key type: %v", err)
	}
	if dictKeyType != keys.TypeString {
		t.Errorf("Expected dictionary key type String, got %v", dictKeyType)
	}

	// Test 6: Verify collection columns are returned correctly
	collectionCols := collectionsTable.GetCollectionColumns()
	expectedCols := []keys.ColKey{listCol, setCol, dictCol}

	if len(collectionCols) != len(expectedCols) {
		t.Errorf("Expected %d collection columns, got %d", len(expectedCols), len(collectionCols))
	}

	// Check if all expected columns are present
	for _, expectedCol := range expectedCols {
		found := false
		for _, actualCol := range collectionCols {
			if actualCol == expectedCol {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Collection column %v not found in result", expectedCol)
		}
	}

	// Test 7: Try to get element type for non-collection column
	idCol, _ := collectionsTable.GetColumn("id")
	_, err = collectionsTable.GetCollectionElementType(idCol)
	if err == nil {
		t.Error("Should fail to get element type for non-collection column")
	}

	// Test 8: Try to get dictionary key type for non-dictionary column
	_, err = collectionsTable.GetDictionaryKeyType(listCol)
	if err == nil {
		t.Error("Should fail to get dictionary key type for non-dictionary column")
	}

	t.Logf("✅ Collection types test passed!")
}

// TestColumnAttributes tests column attribute management
func TestColumnAttributes(t *testing.T) {
	tempFile := "/tmp/test_attributes.turdb"
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
		Name:    "attributes_test",
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
				Nullable: true, // Make nullable for primary key validation test
			},
		},
	}

	attrsTable, err := table.NewTable("attributes_test", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create attributes table: %v", err)
	}

	idCol, _ := attrsTable.GetColumn("id")
	nameCol, _ := attrsTable.GetColumn("name")

	// Test 1: Set indexed attribute
	err = attrsTable.SetColumnAttribute(nameCol, table.ColAttrIndexed)
	if err != nil {
		t.Errorf("Failed to set indexed attribute: %v", err)
	}

	// Verify indexed attribute
	if !attrsTable.HasColumnAttribute(nameCol, table.ColAttrIndexed) {
		t.Error("Column should have indexed attribute")
	}

	// Test 2: Get indexed columns
	indexedCols := attrsTable.GetIndexedColumns()
	if len(indexedCols) != 1 || indexedCols[0] != nameCol {
		t.Errorf("Expected 1 indexed column (name), got %v", indexedCols)
	}

	// Test 3: Set unique attribute
	err = attrsTable.SetColumnAttribute(nameCol, table.ColAttrUnique)
	if err != nil {
		t.Errorf("Failed to set unique attribute: %v", err)
	}

	// Verify unique attribute
	if !attrsTable.HasColumnAttribute(nameCol, table.ColAttrUnique) {
		t.Error("Column should have unique attribute")
	}

	// Test 4: Get unique columns
	uniqueCols := attrsTable.GetUniqueColumns()
	if len(uniqueCols) != 1 || uniqueCols[0] != nameCol {
		t.Errorf("Expected 1 unique column (name), got %v", uniqueCols)
	}

	// Test 5: Clear attribute
	err = attrsTable.ClearColumnAttribute(nameCol, table.ColAttrIndexed)
	if err != nil {
		t.Errorf("Failed to clear indexed attribute: %v", err)
	}

	// Verify attribute was cleared
	if attrsTable.HasColumnAttribute(nameCol, table.ColAttrIndexed) {
		t.Error("Column should not have indexed attribute after clearing")
	}

	// Test 6: Try to set primary key attribute on nullable column
	err = attrsTable.SetColumnAttribute(nameCol, table.ColAttrPrimaryKey)
	if err == nil {
		t.Error("Should fail to set primary key attribute on nullable column")
	}

	// Test 7: Set primary key attribute on non-nullable column
	err = attrsTable.SetColumnAttribute(idCol, table.ColAttrPrimaryKey)
	if err != nil {
		t.Errorf("Failed to set primary key attribute: %v", err)
	}

	// Test 8: Get column attributes
	attrs, err := attrsTable.GetColumnAttributes(nameCol)
	if err != nil {
		t.Errorf("Failed to get column attributes: %v", err)
	}

	if !attrs.IsUnique() {
		t.Error("Column should still have unique attribute")
	}

	t.Logf("✅ Column attributes test passed!")
}