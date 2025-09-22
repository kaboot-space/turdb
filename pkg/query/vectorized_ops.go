package query

import (
	"unsafe"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// VectorizedOperations provides SIMD-style bulk operations for query execution
// Based on realm-core's vectorized comparison patterns
type VectorizedOperations struct {
	chunkSize int // Optimal chunk size for vectorized operations
}

// NewVectorizedOperations creates a new vectorized operations handler
func NewVectorizedOperations() *VectorizedOperations {
	return &VectorizedOperations{
		chunkSize: 64, // Based on realm-core's optimal chunk size
	}
}

// BulkCompareIntegers performs vectorized comparison on integer arrays
// Based on realm-core's template find pattern for integer arrays
func (vo *VectorizedOperations) BulkCompareIntegers(
	values []int64,
	target int64,
	op ComparisonOp,
	results []bool,
) int {
	if len(values) != len(results) {
		return 0
	}

	matchCount := 0
	chunkCount := (len(values) + vo.chunkSize - 1) / vo.chunkSize

	for chunk := 0; chunk < chunkCount; chunk++ {
		start := chunk * vo.chunkSize
		end := start + vo.chunkSize
		if end > len(values) {
			end = len(values)
		}

		// Vectorized comparison within chunk
		matchCount += vo.compareIntegerChunk(values[start:end], target, op, results[start:end])
	}

	return matchCount
}

// compareIntegerChunk performs vectorized comparison on a single chunk
func (vo *VectorizedOperations) compareIntegerChunk(
	chunk []int64,
	target int64,
	op ComparisonOp,
	results []bool,
) int {
	matchCount := 0

	// Unroll loop for better performance (realm-core pattern)
	i := 0
	for i+4 <= len(chunk) {
		// Process 4 integers at once
		results[i] = vo.compareInteger(chunk[i], target, op)
		results[i+1] = vo.compareInteger(chunk[i+1], target, op)
		results[i+2] = vo.compareInteger(chunk[i+2], target, op)
		results[i+3] = vo.compareInteger(chunk[i+3], target, op)

		if results[i] {
			matchCount++
		}
		if results[i+1] {
			matchCount++
		}
		if results[i+2] {
			matchCount++
		}
		if results[i+3] {
			matchCount++
		}

		i += 4
	}

	// Handle remaining elements
	for i < len(chunk) {
		results[i] = vo.compareInteger(chunk[i], target, op)
		if results[i] {
			matchCount++
		}
		i++
	}

	return matchCount
}

// compareInteger performs single integer comparison
func (vo *VectorizedOperations) compareInteger(value, target int64, op ComparisonOp) bool {
	switch op {
	case OpEqual:
		return value == target
	case OpNotEqual:
		return value != target
	case OpLess:
		return value < target
	case OpLessEqual:
		return value <= target
	case OpGreater:
		return value > target
	case OpGreaterEqual:
		return value >= target
	default:
		return false
	}
}

// BulkCompareStrings performs vectorized comparison on string arrays
// Based on realm-core's string comparison patterns
func (vo *VectorizedOperations) BulkCompareStrings(
	values []string,
	target string,
	op ComparisonOp,
	caseSensitive bool,
	results []bool,
) int {
	if len(values) != len(results) {
		return 0
	}

	matchCount := 0
	chunkCount := (len(values) + vo.chunkSize - 1) / vo.chunkSize

	for chunk := 0; chunk < chunkCount; chunk++ {
		start := chunk * vo.chunkSize
		end := start + vo.chunkSize
		if end > len(values) {
			end = len(values)
		}

		// Vectorized string comparison within chunk
		matchCount += vo.compareStringChunk(values[start:end], target, op, caseSensitive, results[start:end])
	}

	return matchCount
}

// compareStringChunk performs vectorized string comparison on a single chunk
func (vo *VectorizedOperations) compareStringChunk(
	chunk []string,
	target string,
	op ComparisonOp,
	caseSensitive bool,
	results []bool,
) int {
	matchCount := 0

	// Process strings in chunks for better cache locality
	for i := 0; i < len(chunk); i++ {
		results[i] = vo.compareString(chunk[i], target, op, caseSensitive)
		if results[i] {
			matchCount++
		}
	}

	return matchCount
}

// compareString performs single string comparison
func (vo *VectorizedOperations) compareString(value, target string, op ComparisonOp, caseSensitive bool) bool {
	if !caseSensitive {
		// Fast case-insensitive comparison using unsafe string conversion
		value = vo.toLowerFast(value)
		target = vo.toLowerFast(target)
	}

	switch op {
	case OpEqual:
		return value == target
	case OpNotEqual:
		return value != target
	case OpLess:
		return value < target
	case OpLessEqual:
		return value <= target
	case OpGreater:
		return value > target
	case OpGreaterEqual:
		return value >= target
	case OpContains:
		return vo.containsString(value, target)
	case OpBeginsWith:
		return vo.beginsWithString(value, target)
	case OpEndsWith:
		return vo.endsWithString(value, target)
	default:
		return false
	}
}

// toLowerFast performs fast case conversion using unsafe operations
func (vo *VectorizedOperations) toLowerFast(s string) string {
	if s == "" {
		return s
	}

	// Convert to byte slice for manipulation
	b := make([]byte, len(s))
	copy(b, s)

	// Fast ASCII lowercase conversion
	for i := 0; i < len(b); i++ {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 32
		}
	}

	return *(*string)(unsafe.Pointer(&b))
}

// containsString checks if value contains target
func (vo *VectorizedOperations) containsString(value, target string) bool {
	if len(target) == 0 {
		return true
	}
	if len(value) < len(target) {
		return false
	}

	// Boyer-Moore-like pattern matching for better performance
	for i := 0; i <= len(value)-len(target); i++ {
		if value[i:i+len(target)] == target {
			return true
		}
	}
	return false
}

// beginsWithString checks if value begins with target
func (vo *VectorizedOperations) beginsWithString(value, target string) bool {
	if len(target) > len(value) {
		return false
	}
	return value[:len(target)] == target
}

// endsWithString checks if value ends with target
func (vo *VectorizedOperations) endsWithString(value, target string) bool {
	if len(target) > len(value) {
		return false
	}
	return value[len(value)-len(target):] == target
}

// BulkFindIntegers finds all matching positions in integer array
// Based on realm-core's bulk find operations
func (vo *VectorizedOperations) BulkFindIntegers(
	tbl *table.Table,
	colKey keys.ColKey,
	target int64,
	op ComparisonOp,
	start, end uint64,
) []uint64 {
	matches := make([]uint64, 0, (end-start)/10) // Pre-allocate with estimated capacity

	chunkSize := uint64(vo.chunkSize)
	for current := start; current < end; current += chunkSize {
		chunkEnd := current + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}

		// Process chunk
		chunkMatches := vo.findIntegerChunk(tbl, colKey, target, op, current, chunkEnd)
		matches = append(matches, chunkMatches...)
	}

	return matches
}

// findIntegerChunk finds matches within a single chunk
func (vo *VectorizedOperations) findIntegerChunk(
	tbl *table.Table,
	colKey keys.ColKey,
	target int64,
	op ComparisonOp,
	start, end uint64,
) []uint64 {
	matches := make([]uint64, 0, end-start)

	for pos := start; pos < end; pos++ {
		objKey := keys.NewObjKey(pos)
		obj, err := tbl.GetObject(objKey)
		if err != nil {
			continue
		}

		value, err := obj.GetInt(colKey)
		if err != nil {
			continue
		}

		if vo.compareInteger(value, target, op) {
			matches = append(matches, pos)
		}
	}

	return matches
}

// BulkFindStrings finds all matching positions in string array
func (vo *VectorizedOperations) BulkFindStrings(
	tbl *table.Table,
	colKey keys.ColKey,
	target string,
	op ComparisonOp,
	caseSensitive bool,
	start, end uint64,
) []uint64 {
	matches := make([]uint64, 0, (end-start)/10)

	chunkSize := uint64(vo.chunkSize)
	for current := start; current < end; current += chunkSize {
		chunkEnd := current + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}

		chunkMatches := vo.findStringChunk(tbl, colKey, target, op, caseSensitive, current, chunkEnd)
		matches = append(matches, chunkMatches...)
	}

	return matches
}

// findStringChunk finds string matches within a single chunk
func (vo *VectorizedOperations) findStringChunk(
	tbl *table.Table,
	colKey keys.ColKey,
	target string,
	op ComparisonOp,
	caseSensitive bool,
	start, end uint64,
) []uint64 {
	matches := make([]uint64, 0, end-start)

	for pos := start; pos < end; pos++ {
		objKey := keys.NewObjKey(pos)
		obj, err := tbl.GetObject(objKey)
		if err != nil {
			continue
		}

		value, err := obj.GetString(colKey)
		if err != nil {
			continue
		}

		if vo.compareString(value, target, op, caseSensitive) {
			matches = append(matches, pos)
		}
	}

	return matches
}