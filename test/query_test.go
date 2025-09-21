package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/query"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/table"
)

func TestQueryBasicOperations(t *testing.T) {
	tempFile := "/tmp/test_query_basic.turdb"
	defer os.Remove(tempFile)

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

	tableSpec := &table.TableSpec{
		Name:    "users",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: false},
			{Name: "age", Type: keys.TypeInt, Nullable: true},
			{Name: "score", Type: keys.TypeDouble, Nullable: true},
		},
	}

	userTable, err := table.NewTable("users", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	idCol, _ := userTable.GetColumn("id")
	nameCol, _ := userTable.GetColumn("name")
	ageCol, _ := userTable.GetColumn("age")
	scoreCol, _ := userTable.GetColumn("score")

	testData := []struct {
		id    int64
		name  string
		age   *int64
		score *float64
	}{
		{1, "Alice", intPtr(25), floatPtr(85.5)},
		{2, "Bob", intPtr(30), floatPtr(92.0)},
		{3, "Charlie", intPtr(25), floatPtr(78.5)},
		{4, "David", intPtr(35), floatPtr(88.0)},
		{5, "Eve", nil, floatPtr(95.5)},
	}

	for _, data := range testData {
		objKey, err := userTable.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj, err := userTable.GetObject(objKey)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		obj.Set(idCol, data.id)
		obj.Set(nameCol, data.name)
		if data.age != nil {
			obj.Set(ageCol, *data.age)
		}
		if data.score != nil {
			obj.Set(scoreCol, *data.score)
		}
	}

	t.Run("FindAll", func(t *testing.T) {
		executor := query.NewQueryExecutor(userTable)
		view, err := executor.FindAll()
		if err != nil {
			t.Fatalf("FindAll failed: %v", err)
		}

		if view.Size() != 5 {
			t.Errorf("Expected 5 objects, got %d", view.Size())
		}
	})

	t.Run("Count", func(t *testing.T) {
		executor := query.NewQueryExecutor(userTable)
		count, err := executor.Count()
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
	})

	t.Run("FindFirst", func(t *testing.T) {
		executor := query.NewQueryExecutor(userTable)
		objKey, err := executor.FindFirst()
		if err != nil {
			t.Fatalf("FindFirst failed: %v", err)
		}

		if objKey == keys.NewObjKey(0) {
			t.Error("Expected valid object key, got empty key")
		}
	})

	t.Run("Limit", func(t *testing.T) {
		executor := query.NewQueryExecutor(userTable)
		view, err := executor.Limit(3).FindAll()
		if err != nil {
			t.Fatalf("Limit query failed: %v", err)
		}

		if view.Size() != 3 {
			t.Errorf("Expected 3 objects with limit, got %d", view.Size())
		}
	})
}

func TestQueryWithExpressions(t *testing.T) {
	tempFile := "/tmp/test_query_expressions.turdb"
	defer os.Remove(tempFile)

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

	tableSpec := &table.TableSpec{
		Name:    "products",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: false},
			{Name: "price", Type: keys.TypeDouble, Nullable: false},
			{Name: "category", Type: keys.TypeString, Nullable: false},
		},
	}

	productTable, err := table.NewTable("products", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	idCol, _ := productTable.GetColumn("id")
	nameCol, _ := productTable.GetColumn("name")
	priceCol, _ := productTable.GetColumn("price")
	categoryCol, _ := productTable.GetColumn("category")

	testProducts := []struct {
		id       int64
		name     string
		price    float64
		category string
	}{
		{1, "Laptop", 999.99, "Electronics"},
		{2, "Book", 19.99, "Books"},
		{3, "Phone", 699.99, "Electronics"},
		{4, "Desk", 299.99, "Furniture"},
		{5, "Chair", 149.99, "Furniture"},
	}

	for _, product := range testProducts {
		objKey, err := productTable.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj, err := productTable.GetObject(objKey)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		obj.Set(idCol, product.id)
		obj.Set(nameCol, product.name)
		obj.Set(priceCol, product.price)
		obj.Set(categoryCol, product.category)
	}

	t.Run("ComparisonExpressions", func(t *testing.T) {
		priceExpr := &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: "price", ColKey: priceCol},
			Operator: query.OpGreater,
			Right:    &query.LiteralExpression{Value: 500.0},
		}

		executor := query.NewQueryExecutor(productTable)
		executor = executor.Where(priceExpr)

		view, err := executor.FindAll()
		if err != nil {
			t.Fatalf("Comparison query failed: %v", err)
		}

		if view.Size() != 2 {
			t.Errorf("Expected 2 products with price > 500, got %d", view.Size())
		}
	})

	t.Run("LogicalExpressions", func(t *testing.T) {
		categoryExpr := &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: "category", ColKey: categoryCol},
			Operator: query.OpEqual,
			Right:    &query.LiteralExpression{Value: "Electronics"},
		}

		priceExpr := &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: "price", ColKey: priceCol},
			Operator: query.OpLess,
			Right:    &query.LiteralExpression{Value: 800.0},
		}

		andExpr := &query.LogicalExpression{
			Left:     categoryExpr,
			Operator: query.OpAnd,
			Right:    priceExpr,
		}

		executor := query.NewQueryExecutor(productTable)
		executor = executor.Where(andExpr)

		view, err := executor.FindAll()
		if err != nil {
			t.Fatalf("Logical AND query failed: %v", err)
		}

		if view.Size() != 1 {
			t.Errorf("Expected 1 product (Electronics AND price < 800), got %d", view.Size())
		}
	})
}

func TestQuerySorting(t *testing.T) {
	tempFile := "/tmp/test_query_sorting.turdb"
	defer os.Remove(tempFile)

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

	tableSpec := &table.TableSpec{
		Name:    "scores",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "name", Type: keys.TypeString, Nullable: false},
			{Name: "score", Type: keys.TypeInt, Nullable: false},
		},
	}

	scoreTable, err := table.NewTable("scores", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	idCol, _ := scoreTable.GetColumn("id")
	nameCol, _ := scoreTable.GetColumn("name")
	scoreCol, _ := scoreTable.GetColumn("score")

	testScores := []struct {
		id    int64
		name  string
		score int64
	}{
		{1, "Alice", 85},
		{2, "Bob", 92},
		{3, "Charlie", 78},
		{4, "David", 88},
		{5, "Eve", 95},
	}

	for _, data := range testScores {
		objKey, err := scoreTable.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj, err := scoreTable.GetObject(objKey)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		obj.Set(idCol, data.id)
		obj.Set(nameCol, data.name)
		obj.Set(scoreCol, data.score)
	}

	t.Run("SortAscending", func(t *testing.T) {
		ordering := query.NewDescriptorOrdering()
		ordering.AddColumn(scoreCol, true) // ascending

		executor := query.NewQueryExecutor(scoreTable)
		executor = executor.Sort(ordering)

		view, err := executor.FindAll()
		if err != nil {
			t.Fatalf("Sort ascending query failed: %v", err)
		}

		if view.Size() != 5 {
			t.Errorf("Expected 5 objects, got %d", view.Size())
		}

		firstObj, err := view.GetObject(0)
		if err != nil {
			t.Fatalf("Failed to get first object: %v", err)
		}

		firstScore, err := firstObj.Get(scoreCol)
		if err != nil {
			t.Fatalf("Failed to get score: %v", err)
		}

		if firstScore.(int64) != 78 {
			t.Errorf("Expected first score to be 78 (lowest), got %v", firstScore)
		}
	})

	t.Run("SortDescending", func(t *testing.T) {
		ordering := query.NewDescriptorOrdering()
		ordering.AddColumn(scoreCol, false) // descending

		executor := query.NewQueryExecutor(scoreTable)
		executor = executor.Sort(ordering)

		view, err := executor.FindAll()
		if err != nil {
			t.Fatalf("Sort descending query failed: %v", err)
		}

		firstObj, err := view.GetObject(0)
		if err != nil {
			t.Fatalf("Failed to get first object: %v", err)
		}

		firstScore, err := firstObj.Get(scoreCol)
		if err != nil {
			t.Fatalf("Failed to get score: %v", err)
		}

		if firstScore.(int64) != 95 {
			t.Errorf("Expected first score to be 95 (highest), got %v", firstScore)
		}
	})
}

func TestTableViewOperations(t *testing.T) {
	tempFile := "/tmp/test_table_view.turdb"
	defer os.Remove(tempFile)

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

	tableSpec := &table.TableSpec{
		Name:    "items",
		Type:    table.TopLevel,
		Version: 1,
		Columns: []table.ColumnSpec{
			{Name: "id", Type: keys.TypeInt, Nullable: false},
			{Name: "value", Type: keys.TypeInt, Nullable: false},
		},
	}

	itemTable, err := table.NewTable("items", tableSpec, group, alloc, fileFormat)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	idCol, _ := itemTable.GetColumn("id")
	valueCol, _ := itemTable.GetColumn("value")

	for i := int64(1); i <= 10; i++ {
		objKey, err := itemTable.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj, err := itemTable.GetObject(objKey)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		obj.Set(idCol, i)
		obj.Set(valueCol, i*10)
	}

	executor := query.NewQueryExecutor(itemTable)
	view, err := executor.FindAll()
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	t.Run("ViewSize", func(t *testing.T) {
		if view.Size() != 10 {
			t.Errorf("Expected view size 10, got %d", view.Size())
		}
	})

	t.Run("ViewIsEmpty", func(t *testing.T) {
		if view.IsEmpty() {
			t.Error("View should not be empty")
		}
	})

	t.Run("ViewGetKey", func(t *testing.T) {
		key := view.GetKey(0)
		if key == keys.NewObjKey(0) {
			t.Error("Expected valid key, got empty key")
		}
	})

	t.Run("ViewGetObject", func(t *testing.T) {
		obj, err := view.GetObject(0)
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}

		if obj == nil {
			t.Error("Expected valid object, got nil")
		}
	})

	t.Run("ViewClear", func(t *testing.T) {
		view.Clear()
		if !view.IsEmpty() {
			t.Error("View should be empty after clear")
		}
	})

	t.Run("ViewDoSync", func(t *testing.T) {
		err := view.DoSync()
		if err != nil {
			t.Fatalf("DoSync failed: %v", err)
		}

		if view.Size() != 10 {
			t.Errorf("Expected view size 10 after sync, got %d", view.Size())
		}
	})
}

func intPtr(i int64) *int64 {
	return &i
}

func floatPtr(f float64) *float64 {
	return &f
}
