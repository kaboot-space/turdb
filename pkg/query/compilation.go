package query

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// QueryCompiler handles runtime code generation for hot queries
type QueryCompiler struct {
	compiledQueries map[string]*CompiledQuery
	hitCounts       map[string]int
	compileThreshold int
	mutex           sync.RWMutex
}

// CompiledQuery represents a compiled query with generated execution code
type CompiledQuery struct {
	ID           string
	Expression   QueryExpression
	GeneratedCode string
	ExecuteFunc  func(*table.Table, keys.ObjKey) bool
	CompileTime  time.Time
	HitCount     int
	LastUsed     time.Time
}

// CompilationStats tracks compilation performance metrics
type CompilationStats struct {
	TotalCompilations int
	CompileTime       time.Duration
	CacheHits         int
	CacheMisses       int
}

// NewQueryCompiler creates a new query compiler
func NewQueryCompiler() *QueryCompiler {
	return &QueryCompiler{
		compiledQueries:  make(map[string]*CompiledQuery),
		hitCounts:        make(map[string]int),
		compileThreshold: 10, // Compile after 10 uses
	}
}

// SetCompileThreshold sets the threshold for query compilation
func (qc *QueryCompiler) SetCompileThreshold(threshold int) {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	qc.compileThreshold = threshold
}

// ShouldCompile determines if a query should be compiled based on usage patterns
func (qc *QueryCompiler) ShouldCompile(queryID string) bool {
	qc.mutex.RLock()
	defer qc.mutex.RUnlock()
	
	count, exists := qc.hitCounts[queryID]
	if !exists {
		return false
	}
	
	return count >= qc.compileThreshold
}

// GetOrCompile retrieves a compiled query or compiles it if needed
func (qc *QueryCompiler) GetOrCompile(queryID string, expr QueryExpression) (*CompiledQuery, error) {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	
	// Increment hit count
	qc.hitCounts[queryID]++
	
	// Check if already compiled
	if compiled, exists := qc.compiledQueries[queryID]; exists {
		compiled.HitCount++
		compiled.LastUsed = time.Now()
		return compiled, nil
	}
	
	// Check if should compile
	if qc.hitCounts[queryID] < qc.compileThreshold {
		return nil, nil // Not ready for compilation
	}
	
	// Compile the query
	compiled, err := qc.compileQuery(queryID, expr)
	if err != nil {
		return nil, fmt.Errorf("compilation failed: %w", err)
	}
	
	qc.compiledQueries[queryID] = compiled
	return compiled, nil
}

// compileQuery generates optimized execution code for a query expression
func (qc *QueryCompiler) compileQuery(queryID string, expr QueryExpression) (*CompiledQuery, error) {
	codeGen := &CodeGenerator{}
	generatedCode, err := codeGen.GenerateCode(expr)
	if err != nil {
		return nil, fmt.Errorf("code generation failed: %w", err)
	}

	// Format the generated code
	formattedCode, err := codeGen.FormatCode(generatedCode)
	if err != nil {
		formattedCode = generatedCode // Use unformatted if formatting fails
	}

	// Create execute function (simplified for now)
	executeFunc, err := qc.createExecuteFunc(formattedCode)
	if err != nil {
		return nil, fmt.Errorf("failed to create execute function: %w", err)
	}

	compiled := &CompiledQuery{
		ID:           queryID,
		Expression:   expr,
		GeneratedCode: formattedCode,
		ExecuteFunc:  executeFunc,
		CompileTime:  time.Now(),
		HitCount:     0,
		LastUsed:     time.Now(),
	}

	return compiled, nil
}

// createExecuteFunc creates an executable function from generated code
func (qc *QueryCompiler) createExecuteFunc(code string) (func(*table.Table, keys.ObjKey) bool, error) {
	// Parse the generated code into AST
	expr, err := parser.ParseExpr(code)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated code: %w", err)
	}

	// Return a function that evaluates the compiled expression
	return func(tbl *table.Table, objKey keys.ObjKey) bool {
		obj, err := tbl.GetObject(objKey)
		if err != nil {
			return false
		}
		result := qc.evaluateCompiledExpression(expr, tbl, obj)
		if boolResult, ok := result.(bool); ok {
			return boolResult
		}
		return false
	}, nil
}

// evaluateCompiledExpression evaluates a compiled AST expression
func (qc *QueryCompiler) evaluateCompiledExpression(node ast.Expr, tbl *table.Table, obj *table.Object) interface{} {
	switch n := node.(type) {
	case *ast.BinaryExpr:
		left := qc.evaluateCompiledExpression(n.X, tbl, obj)
		right := qc.evaluateCompiledExpression(n.Y, tbl, obj)
		return qc.evaluateBinaryOp(left, right, n.Op)
	case *ast.Ident:
		// Handle property access
		return qc.getPropertyValue(obj, n.Name)
	case *ast.BasicLit:
		return qc.parseBasicLit(n)
	default:
		return nil
	}
}

// evaluateBinaryOp evaluates binary operations in compiled expressions
func (qc *QueryCompiler) evaluateBinaryOp(left, right interface{}, op token.Token) interface{} {
	switch op {
	case token.EQL:
		return reflect.DeepEqual(left, right)
	case token.NEQ:
		return !reflect.DeepEqual(left, right)
	case token.LSS:
		return compareValues(left, right) < 0
	case token.LEQ:
		return compareValues(left, right) <= 0
	case token.GTR:
		return compareValues(left, right) > 0
	case token.GEQ:
		return compareValues(left, right) >= 0
	case token.LAND:
		return toBool(left) && toBool(right)
	case token.LOR:
		return toBool(left) || toBool(right)
	default:
		return false
	}
}

// getPropertyValue retrieves property value from object
func (qc *QueryCompiler) getPropertyValue(obj *table.Object, propertyName string) interface{} {
	// This would need to be implemented based on your object property access
	// For now, return nil as placeholder
	return nil
}

// parseBasicLit parses basic literals in compiled expressions
func (qc *QueryCompiler) parseBasicLit(lit *ast.BasicLit) interface{} {
	switch lit.Kind {
	case token.INT:
		// Parse integer
		return lit.Value
	case token.STRING:
		// Parse string (remove quotes)
		return strings.Trim(lit.Value, "\"")
	case token.FLOAT:
		// Parse float
		return lit.Value
	default:
		return lit.Value
	}
}

// GetStats returns compilation statistics
func (qc *QueryCompiler) GetStats() CompilationStats {
	qc.mutex.RLock()
	defer qc.mutex.RUnlock()
	
	totalHits := 0
	for _, compiled := range qc.compiledQueries {
		totalHits += compiled.HitCount
	}
	
	return CompilationStats{
		TotalCompilations: len(qc.compiledQueries),
		CacheHits:         totalHits,
		CacheMisses:       0, // Would need to track this
	}
}

// ClearCache clears the compilation cache
func (qc *QueryCompiler) ClearCache() {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()
	
	qc.compiledQueries = make(map[string]*CompiledQuery)
	qc.hitCounts = make(map[string]int)
}

// CodeGenerator generates Go code from query expressions
type CodeGenerator struct{}

// GenerateCode generates optimized Go code for a query expression
func (cg *CodeGenerator) GenerateCode(expr QueryExpression) (string, error) {
	switch e := expr.(type) {
	case *ComparisonExpression:
		return cg.generateComparisonCode(e)
	case *LogicalExpression:
		return cg.generateLogicalCode(e)
	case *PropertyExpression:
		return cg.generatePropertyCode(e)
	case *LiteralExpression:
		return cg.generateLiteralCode(e)
	default:
		return "", fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// generateComparisonCode generates code for comparison expressions
func (cg *CodeGenerator) generateComparisonCode(expr *ComparisonExpression) (string, error) {
	leftCode, err := cg.GenerateCode(expr.Left)
	if err != nil {
		return "", err
	}
	
	rightCode, err := cg.GenerateCode(expr.Right)
	if err != nil {
		return "", err
	}
	
	var opCode string
	switch expr.Operator {
	case OpEqual:
		opCode = "=="
	case OpNotEqual:
		opCode = "!="
	case OpLess:
		opCode = "<"
	case OpLessEqual:
		opCode = "<="
	case OpGreater:
		opCode = ">"
	case OpGreaterEqual:
		opCode = ">="
	default:
		return "", fmt.Errorf("unsupported comparison operator: %v", expr.Operator)
	}
	
	return fmt.Sprintf("(%s %s %s)", leftCode, opCode, rightCode), nil
}

// generateLogicalCode generates code for logical expressions
func (cg *CodeGenerator) generateLogicalCode(expr *LogicalExpression) (string, error) {
	leftCode, err := cg.GenerateCode(expr.Left)
	if err != nil {
		return "", err
	}
	
	rightCode, err := cg.GenerateCode(expr.Right)
	if err != nil {
		return "", err
	}
	
	var opCode string
	switch expr.Operator {
	case OpAnd:
		opCode = "&&"
	case OpOr:
		opCode = "||"
	default:
		return "", fmt.Errorf("unsupported logical operator: %v", expr.Operator)
	}
	
	return fmt.Sprintf("(%s %s %s)", leftCode, opCode, rightCode), nil
}

// generatePropertyCode generates code for property expressions
func (cg *CodeGenerator) generatePropertyCode(expr *PropertyExpression) (string, error) {
	return fmt.Sprintf("obj.Get(keys.ColKey(%d))", expr.ColKey), nil
}

// generateLiteralCode generates code for literal expressions
func (cg *CodeGenerator) generateLiteralCode(expr *LiteralExpression) (string, error) {
	switch v := expr.Value.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", v), nil
	case int, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	default:
		return "", fmt.Errorf("unsupported literal type: %T", v)
	}
}

// FormatCode formats generated Go code
func (cg *CodeGenerator) FormatCode(code string) (string, error) {
	formatted, err := format.Source([]byte(code))
	if err != nil {
		return code, err // Return original if formatting fails
	}
	return string(formatted), nil
}