package query

import (
	"fmt"
	"sort"
	"sync"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// QueryExecutor represents the query execution engine
type QueryExecutor struct {
	table      *table.Table
	expression QueryExpression
	limit      uint64
	ordering   *DescriptorOrdering
	sourceView *TableView
	mutex      sync.RWMutex
	nodePool   *NodePool
	metrics    *MetricsCollector
}

// TableView represents a view of table objects matching query criteria
// Based on realm-core TableView class
type TableView struct {
	table     *table.Table
	keyValues []keys.ObjKey
	query     *QueryExecutor
	mutex     sync.RWMutex
	inSync    bool
}

// DescriptorOrdering represents sorting criteria for query results
type DescriptorOrdering struct {
	columns   []keys.ColKey
	ascending []bool
}

// NewQueryExecutor creates a new query executor for the given table
func NewQueryExecutor(tbl *table.Table) *QueryExecutor {
	return &QueryExecutor{
		table: tbl,
		limit: ^uint64(0), // Max uint64, equivalent to size_t(-1) in C++
		nodePool: GlobalNodePool,
		metrics: GlobalMetricsCollector,
	}
}

// NewQueryExecutorFromView creates a query executor from an existing TableView
func NewQueryExecutorFromView(view *TableView) *QueryExecutor {
	return &QueryExecutor{
		table:      view.table,
		sourceView: view,
		limit:      ^uint64(0),
	}
}

// Where sets the query expression for filtering
func (qe *QueryExecutor) Where(expr QueryExpression) *QueryExecutor {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()
	qe.expression = expr
	return qe
}

// Limit sets the maximum number of results to return
func (qe *QueryExecutor) Limit(limit uint64) *QueryExecutor {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()
	qe.limit = limit
	return qe
}

// Sort sets the ordering for query results
func (qe *QueryExecutor) Sort(ordering *DescriptorOrdering) *QueryExecutor {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()
	qe.ordering = ordering
	return qe
}

// FindAll executes the query and returns all matching objects as a TableView
// Based on realm-core Query::find_all implementation
func (qe *QueryExecutor) FindAll() (*TableView, error) {
	return qe.FindAllWithLimit(qe.limit)
}

// FindAllWithLimit executes the query with a specific limit
func (qe *QueryExecutor) FindAllWithLimit(limit uint64) (*TableView, error) {
	qe.mutex.RLock()
	defer qe.mutex.RUnlock()

	view := &TableView{
		table:     qe.table,
		keyValues: make([]keys.ObjKey, 0),
		query:     qe,
		inSync:    false,
	}

	err := qe.doFindAll(view, limit)
	if err != nil {
		return nil, err
	}

	// Apply ordering if specified
	if qe.ordering != nil {
		err = view.applyOrdering(qe.ordering)
		if err != nil {
			return nil, err
		}
	}

	view.inSync = true
	return view, nil
}

// FindFirst finds the first object matching the query
// Based on realm-core Query::find_first implementation
func (qe *QueryExecutor) FindFirst() (keys.ObjKey, error) {
	view, err := qe.FindAllWithLimit(1)
	if err != nil {
		return keys.NewObjKey(0), err
	}

	if view.Size() == 0 {
		return keys.NewObjKey(0), fmt.Errorf("no objects found")
	}

	return view.GetKey(0), nil
}

// Count returns the number of objects matching the query
// Based on realm-core Query::count implementation
func (qe *QueryExecutor) Count() (uint64, error) {
	qe.mutex.RLock()
	defer qe.mutex.RUnlock()

	if qe.expression == nil {
		return qe.table.GetObjectCount(), nil
	}

	count := uint64(0)
	visitor := &CountVisitor{
		table: qe.table,
		count: &count,
	}

	// Iterate through all objects and count matches
	objCount := qe.table.GetObjectCount()
	for i := uint64(0); i < objCount; i++ {
		// This is a simplified implementation - in reality we'd need
		// to iterate through actual object keys
		objKey := keys.NewObjKey(i + 1) // Simplified key generation
		if qe.table.ObjectExists(objKey) {
			obj, err := qe.table.GetObject(objKey)
			if err != nil {
				continue
			}

			if qe.evaluateExpression(qe.expression, obj, visitor) {
				count++
			}
		}
	}

	return count, nil
}

// doFindAll performs the actual query execution
// Based on realm-core Query::do_find_all implementation
func (qe *QueryExecutor) doFindAll(view *TableView, limit uint64) error {
	if qe.expression == nil {
		// No filter - return all objects up to limit
		return qe.findAllObjects(view, limit)
	}

	visitor := &EvaluationVisitor{
		table: qe.table,
	}

	count := uint64(0)

	// Get all actual object keys from the table
	objKeys, err := qe.table.GetAllObjectKeys()
	if err != nil {
		return fmt.Errorf("failed to get object keys: %w", err)
	}

	for _, objKey := range objKeys {
		if count >= limit {
			break
		}

		if !qe.table.ObjectExists(objKey) {
			continue
		}

		obj, err := qe.table.GetObject(objKey)
		if err != nil {
			continue
		}

		// Set the object on the visitor for this evaluation
		visitor.obj = obj
		if qe.evaluateExpression(qe.expression, obj, visitor) {
			view.keyValues = append(view.keyValues, objKey)
			count++
		}
	}

	return nil
}

// findAllObjects adds all objects to the view (no filtering)
func (qe *QueryExecutor) findAllObjects(view *TableView, limit uint64) error {
	count := uint64(0)

	// Get all actual object keys from the table
	objKeys, err := qe.table.GetAllObjectKeys()
	if err != nil {
		return fmt.Errorf("failed to get object keys: %w", err)
	}

	// Add all object keys to the view up to limit
	for _, objKey := range objKeys {
		if count >= limit {
			break
		}

		view.keyValues = append(view.keyValues, objKey)
		count++
	}

	return nil
}

// evaluateExpression evaluates a query expression against an object
func (qe *QueryExecutor) evaluateExpression(expr QueryExpression, obj *table.Object, visitor ExpressionVisitor) bool {
	result := expr.Accept(visitor)
	if boolResult, ok := result.(bool); ok {
		return boolResult
	}
	return false
}

// Evaluate evaluates the query expression against an object with the given key
func (qe *QueryExecutor) Evaluate(table *table.Table, objKey keys.ObjKey) bool {
	if qe.expression == nil {
		return true
	}

	obj, err := table.GetObject(objKey)
	if err != nil {
		return false
	}

	visitor := &EvaluationVisitor{
		table: table,
		obj:   obj,
	}

	return qe.evaluateExpression(qe.expression, obj, visitor)
}

// TableView methods

// Size returns the number of objects in the view
func (tv *TableView) Size() uint64 {
	tv.mutex.RLock()
	defer tv.mutex.RUnlock()
	return uint64(len(tv.keyValues))
}

// IsEmpty returns true if the view contains no objects
func (tv *TableView) IsEmpty() bool {
	return tv.Size() == 0
}

// GetKey returns the object key at the specified index
func (tv *TableView) GetKey(index uint64) keys.ObjKey {
	tv.mutex.RLock()
	defer tv.mutex.RUnlock()
	if index >= uint64(len(tv.keyValues)) {
		return keys.NewObjKey(0)
	}
	return tv.keyValues[index]
}

// GetObject returns the object at the specified index
func (tv *TableView) GetObject(index uint64) (*table.Object, error) {
	objKey := tv.GetKey(index)
	if objKey == keys.NewObjKey(0) {
		return nil, fmt.Errorf("index out of bounds")
	}
	return tv.table.GetObject(objKey)
}

// Clear removes all objects from the view
func (tv *TableView) Clear() {
	tv.mutex.Lock()
	defer tv.mutex.Unlock()
	tv.keyValues = tv.keyValues[:0]
	tv.inSync = false
}

// DoSync synchronizes the view with its source
// Based on realm-core TableView::do_sync implementation
func (tv *TableView) DoSync() error {
	tv.mutex.Lock()
	defer tv.mutex.Unlock()

	if tv.query != nil {
		// Re-execute the query to sync
		tv.keyValues = tv.keyValues[:0]
		tv.inSync = false

		err := tv.query.doFindAll(tv, tv.query.limit)
		if err != nil {
			return err
		}

		if tv.query.ordering != nil {
			err = tv.applyOrdering(tv.query.ordering)
			if err != nil {
				return err
			}
		}

		tv.inSync = true
	}

	return nil
}

// IsInSync returns true if the view is synchronized with its source
func (tv *TableView) IsInSync() bool {
	tv.mutex.RLock()
	defer tv.mutex.RUnlock()
	return tv.inSync
}

// applyOrdering sorts the view according to the specified ordering
func (tv *TableView) applyOrdering(ordering *DescriptorOrdering) error {
	if len(ordering.columns) == 0 {
		return nil
	}

	sort.Slice(tv.keyValues, func(i, j int) bool {
		objI, errI := tv.table.GetObject(tv.keyValues[i])
		objJ, errJ := tv.table.GetObject(tv.keyValues[j])

		if errI != nil || errJ != nil {
			return false
		}

		// Compare by each column in order
		for idx, colKey := range ordering.columns {
			valI, errI := objI.Get(colKey)
			valJ, errJ := objJ.Get(colKey)

			if errI != nil || errJ != nil {
				continue
			}

			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if ordering.ascending[idx] {
					return cmp < 0
				}
				return cmp > 0
			}
		}

		return false
	})

	return nil
}

// compareValues compares two values for sorting
func compareValues(a, b interface{}) int {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			} else if va && !vb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// DescriptorOrdering methods

// NewDescriptorOrdering creates a new ordering descriptor
func NewDescriptorOrdering() *DescriptorOrdering {
	return &DescriptorOrdering{
		columns:   make([]keys.ColKey, 0),
		ascending: make([]bool, 0),
	}
}

// AddColumn adds a column to the ordering
func (do *DescriptorOrdering) AddColumn(colKey keys.ColKey, ascending bool) {
	do.columns = append(do.columns, colKey)
	do.ascending = append(do.ascending, ascending)
}

// Visitor implementations for query evaluation

// EvaluationVisitor evaluates expressions during query execution
type EvaluationVisitor struct {
	table *table.Table
	obj   *table.Object
}

// VisitComparison evaluates comparison expressions
func (ev *EvaluationVisitor) VisitComparison(expr *ComparisonExpression) interface{} {
	leftVal := expr.Left.Accept(ev)
	rightVal := expr.Right.Accept(ev)

	return evaluateComparison(leftVal, rightVal, expr.Operator)
}

// VisitLogical evaluates logical expressions
func (ev *EvaluationVisitor) VisitLogical(expr *LogicalExpression) interface{} {
	leftResult := expr.Left.Accept(ev)

	switch expr.Operator {
	case OpAnd:
		if !toBool(leftResult) {
			return false
		}
		rightResult := expr.Right.Accept(ev)
		return toBool(rightResult)
	case OpOr:
		if toBool(leftResult) {
			return true
		}
		rightResult := expr.Right.Accept(ev)
		return toBool(rightResult)
	case OpNot:
		return !toBool(leftResult)
	}

	return false
}

// VisitProperty evaluates property access
func (ev *EvaluationVisitor) VisitProperty(expr *PropertyExpression) interface{} {
	if ev.obj == nil {
		return nil
	}

	value, err := ev.obj.Get(expr.ColKey)
	if err != nil {
		return nil
	}

	return value
}

// VisitLiteral returns literal values
func (ev *EvaluationVisitor) VisitLiteral(expr *LiteralExpression) interface{} {
	return expr.Value
}

// CountVisitor counts matching objects
type CountVisitor struct {
	table *table.Table
	count *uint64
}

// VisitComparison for counting
func (cv *CountVisitor) VisitComparison(expr *ComparisonExpression) interface{} {
	// Simplified counting implementation
	return true
}

// VisitLogical for counting
func (cv *CountVisitor) VisitLogical(expr *LogicalExpression) interface{} {
	return true
}

// VisitProperty for counting
func (cv *CountVisitor) VisitProperty(expr *PropertyExpression) interface{} {
	return nil
}

// VisitLiteral for counting
func (cv *CountVisitor) VisitLiteral(expr *LiteralExpression) interface{} {
	return expr.Value
}

// Helper functions

// toBool converts interface{} to bool
func toBool(val interface{}) bool {
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}

// evaluateComparison evaluates comparison operations
func evaluateComparison(left, right interface{}, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return compareValues(left, right) == 0
	case OpNotEqual:
		return compareValues(left, right) != 0
	case OpLess:
		return compareValues(left, right) < 0
	case OpLessEqual:
		return compareValues(left, right) <= 0
	case OpGreater:
		return compareValues(left, right) > 0
	case OpGreaterEqual:
		return compareValues(left, right) >= 0
	}
	return false
}
