package query

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/turdb/tur/pkg/keys"
)

// PlanCache manages cached query execution plans
type PlanCache struct {
	cache       map[string]*CachedPlan
	mutex       sync.RWMutex
	maxSize     int
	maxAge      time.Duration
	hitCount    uint64
	missCount   uint64
	evictions   uint64
	lastCleanup time.Time
}

// CachedPlan represents a cached query execution plan
type CachedPlan struct {
	Plan       *QueryPlan
	Key        string
	CreatedAt  time.Time
	LastUsed   time.Time
	HitCount   uint64
	Cost       float64
	IndexUsage map[keys.ColKey]bool
	TableHash  uint64 // Hash of table schema for invalidation
}

// CacheStats provides statistics about cache performance
type CacheStats struct {
	Size        int     `json:"size"`
	MaxSize     int     `json:"max_size"`
	HitCount    uint64  `json:"hit_count"`
	MissCount   uint64  `json:"miss_count"`
	HitRate     float64 `json:"hit_rate"`
	Evictions   uint64  `json:"evictions"`
	LastCleanup int64   `json:"last_cleanup"`
}

// NewPlanCache creates a new query plan cache
func NewPlanCache(maxSize int, maxAge time.Duration) *PlanCache {
	return &PlanCache{
		cache:       make(map[string]*CachedPlan),
		maxSize:     maxSize,
		maxAge:      maxAge,
		lastCleanup: time.Now(),
	}
}

// Get retrieves a cached query plan
func (pc *PlanCache) Get(key string, tableHash uint64) (*QueryPlan, bool) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	cached, exists := pc.cache[key]
	if !exists {
		pc.missCount++
		return nil, false
	}

	// Check if plan is still valid (table schema hasn't changed)
	if cached.TableHash != tableHash {
		// Schema changed - plan is invalid
		pc.mutex.RUnlock()
		pc.mutex.Lock()
		delete(pc.cache, key)
		pc.mutex.Unlock()
		pc.mutex.RLock()
		pc.missCount++
		return nil, false
	}

	// Check if plan has expired
	if time.Since(cached.CreatedAt) > pc.maxAge {
		pc.mutex.RUnlock()
		pc.mutex.Lock()
		delete(pc.cache, key)
		pc.mutex.Unlock()
		pc.mutex.RLock()
		pc.missCount++
		return nil, false
	}

	// Update usage statistics
	cached.LastUsed = time.Now()
	cached.HitCount++
	pc.hitCount++

	return cached.Plan, true
}

// Put stores a query plan in the cache
func (pc *PlanCache) Put(key string, plan *QueryPlan, tableHash uint64) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	// Check if we need to evict entries
	if len(pc.cache) >= pc.maxSize {
		pc.evictLRU()
	}

	// Create cached plan
	cached := &CachedPlan{
		Plan:       plan,
		Key:        key,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		HitCount:   0,
		Cost:       plan.totalCost,
		IndexUsage: make(map[keys.ColKey]bool),
		TableHash:  tableHash,
	}

	// Copy index usage information
	for colKey, used := range plan.indexUsage {
		cached.IndexUsage[colKey] = used
	}

	pc.cache[key] = cached
}

// evictLRU evicts the least recently used entry
func (pc *PlanCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, cached := range pc.cache {
		if first || cached.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = cached.LastUsed
			first = false
		}
	}

	if oldestKey != "" {
		delete(pc.cache, oldestKey)
		pc.evictions++
	}
}

// Clear removes all cached plans
func (pc *PlanCache) Clear() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	pc.cache = make(map[string]*CachedPlan)
	pc.hitCount = 0
	pc.missCount = 0
	pc.evictions = 0
}

// Cleanup removes expired entries
func (pc *PlanCache) Cleanup() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, cached := range pc.cache {
		if now.Sub(cached.CreatedAt) > pc.maxAge {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(pc.cache, key)
		pc.evictions++
	}

	pc.lastCleanup = now
}

// GetStats returns cache performance statistics
func (pc *PlanCache) GetStats() *CacheStats {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	total := pc.hitCount + pc.missCount
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(pc.hitCount) / float64(total)
	}

	return &CacheStats{
		Size:        len(pc.cache),
		MaxSize:     pc.maxSize,
		HitCount:    pc.hitCount,
		MissCount:   pc.missCount,
		HitRate:     hitRate,
		Evictions:   pc.evictions,
		LastCleanup: pc.lastCleanup.Unix(),
	}
}

// InvalidateByTable invalidates all plans for a specific table
func (pc *PlanCache) InvalidateByTable(tableHash uint64) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	keysToDelete := make([]string, 0)

	for key, cached := range pc.cache {
		if cached.TableHash == tableHash {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		delete(pc.cache, key)
	}
}

// GenerateKey generates a cache key for a query expression
func (pc *PlanCache) GenerateKey(expr QueryExpression, tableHash uint64) string {
	// Create a string representation of the query
	queryStr := pc.expressionToString(expr)

	// Include table hash to ensure schema changes invalidate cache
	combined := fmt.Sprintf("%s|%d", queryStr, tableHash)

	// Generate SHA256 hash for consistent key
	hash := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(hash[:])
}

// expressionToString converts a query expression to a string representation
func (pc *PlanCache) expressionToString(expr QueryExpression) string {
	switch e := expr.(type) {
	case *ComparisonExpression:
		return fmt.Sprintf("cmp(%s,%s,%v)",
			pc.expressionToString(e.Left),
			pc.operatorToString(e.Operator),
			e.Right)

	case *LogicalExpression:
		return fmt.Sprintf("log(%s,%s,%s)",
			pc.expressionToString(e.Left),
			pc.logicalOpToString(e.Operator),
			pc.expressionToString(e.Right))

	case *PropertyExpression:
		return fmt.Sprintf("prop(%d)", e.ColKey)

	case *LiteralExpression:
		return fmt.Sprintf("lit(%v)", e.Value)

	default:
		return "unknown"
	}
}

// operatorToString converts comparison operator to string
func (pc *PlanCache) operatorToString(op ComparisonOp) string {
	switch op {
	case OpEqual:
		return "eq"
	case OpNotEqual:
		return "ne"
	case OpLess:
		return "lt"
	case OpLessEqual:
		return "le"
	case OpGreater:
		return "gt"
	case OpGreaterEqual:
		return "ge"
	case OpContains:
		return "contains"
	case OpBeginsWith:
		return "begins"
	case OpEndsWith:
		return "ends"
	case OpLike:
		return "like"
	case OpIn:
		return "in"
	case OpBetween:
		return "between"
	default:
		return "unknown"
	}
}

// logicalOpToString converts logical operator to string
func (pc *PlanCache) logicalOpToString(op LogicalOp) string {
	switch op {
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	case OpNot:
		return "not"
	default:
		return "unknown"
	}
}

// GetCachedPlan retrieves a specific cached plan by key
func (pc *PlanCache) GetCachedPlan(key string) (*CachedPlan, bool) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	cached, exists := pc.cache[key]
	return cached, exists
}

// ListCachedPlans returns all cached plans (for debugging)
func (pc *PlanCache) ListCachedPlans() []*CachedPlan {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	plans := make([]*CachedPlan, 0, len(pc.cache))
	for _, cached := range pc.cache {
		plans = append(plans, cached)
	}

	return plans
}

// SetMaxSize updates the maximum cache size
func (pc *PlanCache) SetMaxSize(maxSize int) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	pc.maxSize = maxSize

	// Evict entries if current size exceeds new max
	for len(pc.cache) > pc.maxSize {
		pc.evictLRU()
	}
}

// SetMaxAge updates the maximum age for cached plans
func (pc *PlanCache) SetMaxAge(maxAge time.Duration) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	pc.maxAge = maxAge
}

// AdaptiveCacheConfig controls adaptive caching behavior
// Based on realm-core's query execution patterns
type AdaptiveCacheConfig struct {
	MinExecutionTime time.Duration // Only cache plans that take longer than this
	MaxPlanAge       time.Duration // Maximum age before plan validation
	CostThreshold    float64       // Only cache plans above this cost threshold
	HitRateThreshold float64       // Evict plans below this hit rate
}

// PlanValidationResult indicates if a cached plan is still valid
type PlanValidationResult struct {
	IsValid     bool
	Reason      string
	ShouldEvict bool
	NewCost     float64
}

// ValidateCachedPlan checks if a cached plan is still optimal
// Based on realm-core's query optimization validation
func (pc *PlanCache) ValidateCachedPlan(cached *CachedPlan, currentTableHash uint64) *PlanValidationResult {
	result := &PlanValidationResult{
		IsValid: true,
	}

	// Check table schema changes
	if cached.TableHash != currentTableHash {
		result.IsValid = false
		result.Reason = "table schema changed"
		result.ShouldEvict = true
		return result
	}

	// Check plan age
	if time.Since(cached.CreatedAt) > pc.maxAge {
		result.IsValid = false
		result.Reason = "plan expired"
		result.ShouldEvict = true
		return result
	}

	// Check hit rate performance
	if cached.HitCount > 10 { // Only check after sufficient usage
		hitRate := float64(cached.HitCount) / float64(cached.HitCount+1)
		if hitRate < 0.1 { // Low hit rate threshold
			result.IsValid = false
			result.Reason = "low hit rate"
			result.ShouldEvict = true
			return result
		}
	}

	return result
}

// GetWithValidation retrieves a plan and validates its current optimality
// Similar to realm-core's query execution with plan validation
func (pc *PlanCache) GetWithValidation(key string, tableHash uint64) (*QueryPlan, bool, *PlanValidationResult) {
	pc.mutex.RLock()
	cached, exists := pc.cache[key]
	pc.mutex.RUnlock()

	if !exists {
		pc.missCount++
		return nil, false, nil
	}

	// Validate the cached plan
	validation := pc.ValidateCachedPlan(cached, tableHash)

	if !validation.IsValid {
		if validation.ShouldEvict {
			pc.mutex.Lock()
			delete(pc.cache, key)
			pc.evictions++
			pc.mutex.Unlock()
		}
		pc.missCount++
		return nil, false, validation
	}

	// Update usage statistics
	pc.mutex.Lock()
	cached.LastUsed = time.Now()
	cached.HitCount++
	pc.hitCount++
	pc.mutex.Unlock()

	return cached.Plan, true, validation
}

// PutWithAdaptivePolicy stores a plan using adaptive caching policy
// Based on realm-core's intelligent query plan management
func (pc *PlanCache) PutWithAdaptivePolicy(key string, plan *QueryPlan, tableHash uint64, executionTime time.Duration, config *AdaptiveCacheConfig) bool {
	// Apply adaptive caching rules
	if config != nil {
		// Don't cache fast queries
		if executionTime < config.MinExecutionTime {
			return false
		}

		// Don't cache low-cost plans
		if plan.totalCost < config.CostThreshold {
			return false
		}
	}

	pc.Put(key, plan, tableHash)
	return true
}

// OptimizeCache performs intelligent cache optimization
// Based on realm-core's memory management patterns
func (pc *PlanCache) OptimizeCache() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	now := time.Now()
	toEvict := make([]string, 0)

	// Collect plans for eviction based on multiple criteria
	for key, cached := range pc.cache {
		shouldEvict := false

		// Age-based eviction
		if now.Sub(cached.CreatedAt) > pc.maxAge {
			shouldEvict = true
		}

		// Performance-based eviction (low hit rate)
		if cached.HitCount > 5 {
			timeSinceCreation := now.Sub(cached.CreatedAt)
			expectedHits := timeSinceCreation.Minutes() // Rough heuristic
			actualHitRate := float64(cached.HitCount) / expectedHits

			if actualHitRate < 0.1 { // Very low hit rate
				shouldEvict = true
			}
		}

		// Cost-based eviction (very low cost plans)
		if cached.Cost < 1.0 { // Trivial cost threshold
			shouldEvict = true
		}

		if shouldEvict {
			toEvict = append(toEvict, key)
		}
	}

	// Evict selected plans
	for _, key := range toEvict {
		delete(pc.cache, key)
		pc.evictions++
	}

	pc.lastCleanup = now
}

// GetPlanMetrics returns detailed metrics for a specific plan
// Based on realm-core's query performance tracking
func (pc *PlanCache) GetPlanMetrics(key string) *PlanMetrics {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	cached, exists := pc.cache[key]
	if !exists {
		return nil
	}

	return &PlanMetrics{
		Key:             key,
		Cost:            cached.Cost,
		HitCount:        cached.HitCount,
		Age:             time.Since(cached.CreatedAt),
		LastUsed:        cached.LastUsed,
		IndexUsageCount: len(cached.IndexUsage),
		EfficiencyScore: pc.calculateEfficiencyScore(cached),
	}
}

// PlanMetrics provides detailed performance metrics for a cached plan
type PlanMetrics struct {
	Key             string        `json:"key"`
	Cost            float64       `json:"cost"`
	HitCount        uint64        `json:"hit_count"`
	Age             time.Duration `json:"age"`
	LastUsed        time.Time     `json:"last_used"`
	IndexUsageCount int           `json:"index_usage_count"`
	EfficiencyScore float64       `json:"efficiency_score"`
}

// calculateEfficiencyScore computes an efficiency score for a cached plan
// Based on hit rate, cost, and age factors
func (pc *PlanCache) calculateEfficiencyScore(cached *CachedPlan) float64 {
	if cached.HitCount == 0 {
		return 0.0
	}

	// Base score from hit count
	hitScore := float64(cached.HitCount) / 10.0 // Normalize to reasonable range
	if hitScore > 1.0 {
		hitScore = 1.0
	}

	// Cost factor (lower cost = higher efficiency)
	costFactor := 1.0 / (1.0 + cached.Cost/100.0)

	// Age factor (newer plans get slight bonus)
	age := time.Since(cached.CreatedAt)
	ageFactor := 1.0 / (1.0 + age.Hours()/24.0) // Decay over days

	// Index usage bonus
	indexBonus := float64(len(cached.IndexUsage)) * 0.1

	return (hitScore * costFactor * ageFactor) + indexBonus
}
