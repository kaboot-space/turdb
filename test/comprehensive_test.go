package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/table"
)

func TestComprehensiveInsertAndQuery(t *testing.T) {
	tempFile := "/tmp/test_comprehensive.turdb"
	defer os.Remove(tempFile)

	// Initialize storage
	fileFormat, err := storage.NewFileFormat(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file format: %v", err)
	}
	defer fileFormat.Close()

	err = fileFormat.Initialize(4096)
	if err != nil {
		t.Fatalf("Failed to initialize file format: %v", err)
	}

	err = fileFormat.Open()
	if err != nil {
		t.Fatalf("Failed to open file format: %v", err)
	}

	alloc := core.NewAllocator(fileFormat)
	group, err := storage.NewGroup(fileFormat)
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Create products table
	productSpec := &table.TableSpec{
		Name:    "products",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: false},
			{Name: "category", Type: keys.TypeString, Nullable: false},
			{Name: "price", Type: keys.TypeDouble, Nullable: false},
			{Name: "stock", Type: keys.TypeInt, Nullable: false},
			{Name: "description", Type: keys.TypeString, Nullable: true},
		},
	}

	productTable, err := table.NewTable("products", productSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	idCol, _ := productTable.GetColumn("id")
	err = productTable.SetPrimaryKeyColumn(idCol)
	if err != nil {
		t.Fatalf("Failed to set primary key: %v", err)
	}

	// Insert test data
	products := []struct {
		id          int64
		name        string
		category    string
		price       float64
		stock       int64
		description *string
	}{
		{1, "Laptop", "Electronics", 999.99, 10, stringPtr("High-performance laptop")},
		{2, "Mouse", "Electronics", 29.99, 50, stringPtr("Wireless mouse")},
		{3, "Keyboard", "Electronics", 79.99, 25, nil},
		{4, "Book", "Education", 19.99, 100, stringPtr("Programming guide")},
		{5, "Pen", "Office", 2.99, 200, nil},
		{6, "Monitor", "Electronics", 299.99, 15, stringPtr("24-inch display")},
		{7, "Desk", "Furniture", 199.99, 5, stringPtr("Wooden desk")},
		{8, "Chair", "Furniture", 149.99, 8, stringPtr("Ergonomic chair")},
	}

	var objKeys []keys.ObjKey
	for _, product := range products {
		objKey, err := productTable.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object for product %d: %v", product.id, err)
		}

		err = productTable.SetValue(objKey, "id", product.id)
		if err != nil {
			t.Fatalf("Failed to set id for product %d: %v", product.id, err)
		}

		err = productTable.SetValue(objKey, "name", product.name)
		if err != nil {
			t.Fatalf("Failed to set name for product %d: %v", product.id, err)
		}

		err = productTable.SetValue(objKey, "category", product.category)
		if err != nil {
			t.Fatalf("Failed to set category for product %d: %v", product.id, err)
		}

		err = productTable.SetValue(objKey, "price", product.price)
		if err != nil {
			t.Fatalf("Failed to set price for product %d: %v", product.id, err)
		}

		err = productTable.SetValue(objKey, "stock", product.stock)
		if err != nil {
			t.Fatalf("Failed to set stock for product %d: %v", product.id, err)
		}

		if product.description != nil {
			err = productTable.SetValue(objKey, "description", *product.description)
			if err != nil {
				t.Fatalf("Failed to set description for product %d: %v", product.id, err)
			}
		}

		objKeys = append(objKeys, objKey)
	}

	t.Logf("Successfully inserted %d products", len(products))

	// Test basic iterator using object keys
	t.Run("BasicIterator", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		count := 0
		for _, key := range objKeys {
			name, err := productTable.GetValue(key, "name")
			if err != nil {
				t.Errorf("Failed to get name for key %v: %v", key, err)
				continue
			}
			
			price, err := productTable.GetValue(key, "price")
			if err != nil {
				t.Errorf("Failed to get price for key %v: %v", key, err)
				continue
			}
			
			t.Logf("Product: %s, Price: $%.2f", name, price)
			count++
		}

		if count != len(products) {
			t.Errorf("Expected %d products, found %d", len(products), count)
		}
	})

	// Test chunked processing
	t.Run("ChunkedProcessing", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		chunkSize := 3
		totalCount := 0
		chunkNum := 0
		
		for i := 0; i < len(objKeys); i += chunkSize {
			end := i + chunkSize
			if end > len(objKeys) {
				end = len(objKeys)
			}
			chunk := objKeys[i:end]
			
			chunkNum++
			t.Logf("Processing chunk %d with %d items", chunkNum, len(chunk))
			
			for _, key := range chunk {
				name, err := productTable.GetValue(key, "name")
				if err != nil {
					t.Errorf("Failed to get name for key %v: %v", key, err)
					continue
				}
				
				category, err := productTable.GetValue(key, "category")
				if err != nil {
					t.Errorf("Failed to get category for key %v: %v", key, err)
					continue
				}
				
				t.Logf("  - %s (%s)", name, category)
				totalCount++
			}
		}

		if totalCount != len(products) {
			t.Errorf("Expected %d products, found %d", len(products), totalCount)
		}
	})

	// Test lazy access
	t.Run("LazyAccess", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		t.Logf("Total objects: %d", len(objKeys))

		// Get first 3 items
		for i := 0; i < 3 && i < len(objKeys); i++ {
			key := objKeys[i]
			
			name, err := productTable.GetValue(key, "name")
			if err != nil {
				t.Errorf("Failed to get name for index %d: %v", i, err)
				continue
			}
			
			t.Logf("Item %d: %s", i, name)
		}
	})

	// Test streaming-like processing
	t.Run("StreamingProcessing", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		count := 0
		for _, key := range objKeys {
			name, err := productTable.GetValue(key, "name")
			if err != nil {
				t.Errorf("Failed to get name: %v", err)
				continue
			}
			
			t.Logf("Processing: %s", name)
			count++
		}
		
		t.Logf("Processed %d items", count)
	})

	// Test limited processing
	t.Run("LimitedProcessing", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}
		
		count := 0
		limit := 3
		for _, key := range objKeys {
			if count >= limit {
				t.Log("Reached limit, stopping processing")
				break
			}
			
			name, err := productTable.GetValue(key, "name")
			if err != nil {
				t.Errorf("Failed to get name: %v", err)
				continue
			}
			
			t.Logf("Limited processing result: %s", name)
			count++
		}
		
		if count > limit {
			t.Errorf("Expected limit of %d items, but got %d", limit, count)
		}
	})

	// Test category filtering (manual filtering example)
	t.Run("CategoryFiltering", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		electronicsCount := 0
		furnitureCount := 0

		for _, key := range objKeys {
			category, err := productTable.GetValue(key, "category")
			if err != nil {
				t.Errorf("Failed to get category: %v", err)
				continue
			}
			
			switch category {
			case "Electronics":
				electronicsCount++
				name, _ := productTable.GetValue(key, "name")
				price, _ := productTable.GetValue(key, "price")
				t.Logf("Electronics: %s - $%.2f", name, price)
			case "Furniture":
				furnitureCount++
				name, _ := productTable.GetValue(key, "name")
				price, _ := productTable.GetValue(key, "price")
				t.Logf("Furniture: %s - $%.2f", name, price)
			}
		}

		t.Logf("Found %d electronics and %d furniture items", electronicsCount, furnitureCount)
		
		if electronicsCount != 4 {
			t.Errorf("Expected 4 electronics items, found %d", electronicsCount)
		}
		
		if furnitureCount != 2 {
			t.Errorf("Expected 2 furniture items, found %d", furnitureCount)
		}
	})

	// Test price range filtering
	t.Run("PriceRangeFiltering", func(t *testing.T) {
		objKeys, err := productTable.GetAllObjectKeys()
		if err != nil {
			t.Fatalf("Failed to get object keys: %v", err)
		}

		expensiveCount := 0
		affordableCount := 0
		priceThreshold := 100.0

		for _, key := range objKeys {
			price, err := productTable.GetValue(key, "price")
			if err != nil {
				t.Errorf("Failed to get price: %v", err)
				continue
			}
			
			priceFloat, ok := price.(float64)
			if !ok {
				t.Errorf("Price is not float64: %T", price)
				continue
			}
			
			name, _ := productTable.GetValue(key, "name")
			
			if priceFloat >= priceThreshold {
				expensiveCount++
				t.Logf("Expensive: %s - $%.2f", name, priceFloat)
			} else {
				affordableCount++
				t.Logf("Affordable: %s - $%.2f", name, priceFloat)
			}
		}

		t.Logf("Found %d expensive (>=$%.2f) and %d affordable items", expensiveCount, priceThreshold, affordableCount)
	})
}

func stringPtr(s string) *string {
	return &s
}