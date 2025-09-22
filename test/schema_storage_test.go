package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/schema"
	"github.com/turdb/tur/pkg/storage"
)

func TestSchemaStorage(t *testing.T) {
	// Create temporary directory for test
	tempDir := filepath.Join(os.TempDir(), "tur_schema_test")
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create file format
	fileFormat, err := storage.NewFileFormat(filepath.Join(tempDir, "test.tur"))
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}

	err = fileFormat.Initialize(4096)
	if err != nil {
		t.Fatalf("Failed to initialize file format: %v", err)
	}

	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open file format: %v", err)
	}
	defer fileFormat.Close()

	// Create storage group
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create storage group: %v", err)
	}

	// Create schema storage
	schemaStorage := schema.NewSchemaStorage(group)

	t.Run("CreateAndStore", func(t *testing.T) {
		testCreateAndStoreSchema(t, schemaStorage)
	})

	t.Run("ReadAndValidate", func(t *testing.T) {
		testReadAndValidateSchema(t, schemaStorage)
	})

	t.Run("ReopenAndLoad", func(t *testing.T) {
		// Test reopening and loading schema from disk
		testReopenAndLoadSchema(t, schemaStorage, tempDir)
	})

	t.Run("RemoveSchema", func(t *testing.T) {
		testRemoveSchema(t, schemaStorage)
	})
}

func testCreateAndStoreSchema(t *testing.T, schemaStorage *schema.SchemaStorage) {
	// Create test schema
	testSchema := createTestSchema()

	// Store schema
	err := schemaStorage.StoreSchema("main", testSchema)
	if err != nil {
		t.Fatalf("Failed to store schema: %v", err)
	}
}

func testReadAndValidateSchema(t *testing.T, schemaStorage *schema.SchemaStorage) {
	// Load schema
	loadedSchema, err := schemaStorage.LoadSchema("main")
	if err != nil {
		t.Fatalf("Failed to load schema: %v", err)
	}

	if loadedSchema == nil {
		t.Fatal("Loaded schema is nil")
	}

	// Validate schema properties
	if loadedSchema.Size() == 0 {
		t.Error("Schema should have object schemas")
	}
}

func testReopenAndLoadSchema(t *testing.T, schemaStorage *schema.SchemaStorage, tempDir string) {
	// First store a schema to ensure it exists
	testSchema := createTestSchema()
	err := schemaStorage.StoreSchema("reopen_test", testSchema)
	if err != nil {
		t.Fatalf("Failed to store schema for reopen test: %v", err)
	}

	// Create new file format instance
	fileFormat, err := storage.NewFileFormat(filepath.Join(tempDir, "test.tur"))
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}

	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to reopen file format: %v", err)
	}
	defer fileFormat.Close()

	// Get the group reference from the file format and load the existing group
	groupRef := fileFormat.GetGroupRef()
	if groupRef == 0 {
		t.Fatalf("No group reference found in reopened file")
	}
	
	group, err := storage.LoadGroup(fileFormat, storage.Ref(groupRef))
	if err != nil {
		t.Fatalf("Failed to load storage group: %v", err)
	}

	// Create new schema storage
	newSchemaStorage := schema.NewSchemaStorage(group)

	// Load schema from reopened storage
	loadedSchema, err := newSchemaStorage.LoadSchema("reopen_test")
	if err != nil {
		t.Fatalf("Failed to load schema after reopen: %v", err)
	}

	if loadedSchema == nil {
		t.Fatal("Loaded schema after reopen is nil")
	}
}

func testRemoveSchema(t *testing.T, schemaStorage *schema.SchemaStorage) {
	// Remove schema
	err := schemaStorage.RemoveSchema("main")
	if err != nil {
		t.Fatalf("Failed to remove schema: %v", err)
	}

	// Verify schema is removed
	_, err = schemaStorage.LoadSchema("main")
	if err == nil {
		t.Error("Schema should be removed")
	}
}

func createTestSchema() *schema.Schema {
	// Create test object schema
	objectSchema := createTestObjectSchema()

	// Create schema with the object schema
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createTestObjectSchema() *schema.ObjectSchema {
	// Create properties with correct data types
	idProperty := schema.Property{
		Name:       "id",
		PublicName: "id",
		Type:       keys.TypeInt,
		IsPrimary:  true,
		IsIndexed:  true,
		IsNullable: false,
		IsRequired: true,
	}

	nameProperty := schema.Property{
		Name:       "name",
		PublicName: "name",
		Type:       keys.TypeString,
		IsPrimary:  false,
		IsIndexed:  false,
		IsNullable: true,
		IsRequired: false,
	}

	// Create object schema with correct signature
	objectSchema := schema.NewObjectSchema(
		"TestObject",
		schema.ObjectTypeTopLevel,
		[]schema.Property{idProperty, nameProperty},
		[]schema.Property{}, // computed properties
		"",                  // alias
	)

	return objectSchema
}