package btree

import (
	"container/list"
	"sync"
	"time"
)

// CacheEntry represents a cached leaf node with metadata
type CacheEntry struct {
	node       *BTreeNode
	lastAccess time.Time
	accessCount int64
	dirty      bool
}

// LeafCache implements an LRU cache for B-tree leaf nodes
type LeafCache struct {
	capacity    int
	cache       map[uint64]*list.Element
	lruList     *list.List
	mutex       sync.RWMutex
	hitCount    int64
	missCount   int64
	evictCount  int64
}

// NewLeafCache creates a new leaf node cache with specified capacity
func NewLeafCache(capacity int) *LeafCache {
	if capacity <= 0 {
		capacity = 100 // Default capacity
	}
	
	return &LeafCache{
		capacity: capacity,
		cache:    make(map[uint64]*list.Element),
		lruList:  list.New(),
	}
}

// Get retrieves a leaf node from cache
func (lc *LeafCache) Get(nodeID uint64) (*BTreeNode, bool) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	if elem, exists := lc.cache[nodeID]; exists {
		// Move to front (most recently used)
		lc.lruList.MoveToFront(elem)
		
		entry := elem.Value.(*CacheEntry)
		entry.lastAccess = time.Now()
		entry.accessCount++
		
		lc.hitCount++
		return entry.node, true
	}
	
	lc.missCount++
	return nil, false
}

// Put adds a leaf node to cache
func (lc *LeafCache) Put(nodeID uint64, node *BTreeNode) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	// Check if node already exists
	if elem, exists := lc.cache[nodeID]; exists {
		// Update existing entry
		lc.lruList.MoveToFront(elem)
		entry := elem.Value.(*CacheEntry)
		entry.node = node
		entry.lastAccess = time.Now()
		entry.accessCount++
		return
	}
	
	// Create new entry
	entry := &CacheEntry{
		node:       node,
		lastAccess: time.Now(),
		accessCount: 1,
		dirty:      false,
	}
	
	elem := lc.lruList.PushFront(entry)
	lc.cache[nodeID] = elem
	
	// Evict if over capacity
	if lc.lruList.Len() > lc.capacity {
		lc.evictLRU()
	}
}

// MarkDirty marks a cached node as dirty (needs to be written to disk)
func (lc *LeafCache) MarkDirty(nodeID uint64) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	if elem, exists := lc.cache[nodeID]; exists {
		entry := elem.Value.(*CacheEntry)
		entry.dirty = true
	}
}

// evictLRU removes the least recently used entry
func (lc *LeafCache) evictLRU() {
	elem := lc.lruList.Back()
	if elem != nil {
		lc.lruList.Remove(elem)
		
		// Find and remove from cache map
		for nodeID, cacheElem := range lc.cache {
			if cacheElem == elem {
				delete(lc.cache, nodeID)
				break
			}
		}
		
		lc.evictCount++
	}
}

// Remove removes a specific node from cache
func (lc *LeafCache) Remove(nodeID uint64) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	if elem, exists := lc.cache[nodeID]; exists {
		lc.lruList.Remove(elem)
		delete(lc.cache, nodeID)
	}
}

// Clear removes all entries from cache
func (lc *LeafCache) Clear() {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	lc.cache = make(map[uint64]*list.Element)
	lc.lruList = list.New()
}

// Size returns the current number of cached entries
func (lc *LeafCache) Size() int {
	lc.mutex.RLock()
	defer lc.mutex.RUnlock()
	
	return len(lc.cache)
}

// Stats returns cache statistics
func (lc *LeafCache) Stats() CacheStats {
	lc.mutex.RLock()
	defer lc.mutex.RUnlock()
	
	total := lc.hitCount + lc.missCount
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(lc.hitCount) / float64(total)
	}
	
	return CacheStats{
		Capacity:   lc.capacity,
		Size:       len(lc.cache),
		HitCount:   lc.hitCount,
		MissCount:  lc.missCount,
		EvictCount: lc.evictCount,
		HitRate:    hitRate,
	}
}

// CacheStats represents cache performance statistics
type CacheStats struct {
	Capacity   int
	Size       int
	HitCount   int64
	MissCount  int64
	EvictCount int64
	HitRate    float64
}

// GetDirtyNodes returns all dirty nodes that need to be written to disk
func (lc *LeafCache) GetDirtyNodes() map[uint64]*BTreeNode {
	lc.mutex.RLock()
	defer lc.mutex.RUnlock()
	
	dirtyNodes := make(map[uint64]*BTreeNode)
	
	for nodeID, elem := range lc.cache {
		entry := elem.Value.(*CacheEntry)
		if entry.dirty {
			dirtyNodes[nodeID] = entry.node
		}
	}
	
	return dirtyNodes
}

// FlushDirtyNodes marks all dirty nodes as clean
func (lc *LeafCache) FlushDirtyNodes() {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	
	for _, elem := range lc.cache {
		entry := elem.Value.(*CacheEntry)
		entry.dirty = false
	}
}

// Prefetch loads multiple nodes into cache for sequential access
func (lc *LeafCache) Prefetch(nodeIDs []uint64, loader func(uint64) (*BTreeNode, error)) error {
	for _, nodeID := range nodeIDs {
		if _, exists := lc.Get(nodeID); !exists {
			node, err := loader(nodeID)
			if err != nil {
				return err
			}
			lc.Put(nodeID, node)
		}
	}
	return nil
}