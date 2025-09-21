package table

import (
	"fmt"
	"sync"

	"github.com/turdb/tur/pkg/keys"
)

// BacklinkManager manages backlink relationships between tables
type BacklinkManager struct {
	mutex     sync.RWMutex
	backlinks map[keys.TableKey]map[keys.ColKey]*keys.BacklinkInfo
	registry  map[keys.TableKey]*Table // Table registry for backlink updates
}

// NewBacklinkManager creates a new backlink manager
func NewBacklinkManager() *BacklinkManager {
	return &BacklinkManager{
		backlinks: make(map[keys.TableKey]map[keys.ColKey]*keys.BacklinkInfo),
		registry:  make(map[keys.TableKey]*Table),
	}
}

// RegisterTable registers a table with the backlink manager
func (bm *BacklinkManager) RegisterTable(table *Table) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	
	bm.registry[table.key] = table
}

// UnregisterTable unregisters a table from the backlink manager
func (bm *BacklinkManager) UnregisterTable(tableKey keys.TableKey) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	
	delete(bm.registry, tableKey)
	delete(bm.backlinks, tableKey)
}

// AddBacklink adds a backlink relationship
func (bm *BacklinkManager) AddBacklink(targetTable keys.TableKey, backlinkCol keys.ColKey, 
	originTable keys.TableKey, originCol keys.ColKey) error {
	
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	
	// Initialize backlinks map for target table if needed
	if bm.backlinks[targetTable] == nil {
		bm.backlinks[targetTable] = make(map[keys.ColKey]*keys.BacklinkInfo)
	}
	
	// Create backlink info
	backlinkInfo := keys.NewBacklinkInfo(originTable, originCol, targetTable)
	bm.backlinks[targetTable][backlinkCol] = &backlinkInfo
	
	return nil
}

// RemoveBacklink removes a backlink relationship
func (bm *BacklinkManager) RemoveBacklink(targetTable keys.TableKey, backlinkCol keys.ColKey) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	
	if tableBacklinks, exists := bm.backlinks[targetTable]; exists {
		delete(tableBacklinks, backlinkCol)
		
		// Clean up empty table entry
		if len(tableBacklinks) == 0 {
			delete(bm.backlinks, targetTable)
		}
	}
	
	return nil
}

// GetBacklinkInfo retrieves backlink information for a column
func (bm *BacklinkManager) GetBacklinkInfo(targetTable keys.TableKey, backlinkCol keys.ColKey) (*keys.BacklinkInfo, bool) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	if tableBacklinks, exists := bm.backlinks[targetTable]; exists {
		if info, exists := tableBacklinks[backlinkCol]; exists {
			return info, true
		}
	}
	
	return nil, false
}

// GetTableBacklinks returns all backlink columns for a table
func (bm *BacklinkManager) GetTableBacklinks(tableKey keys.TableKey) map[keys.ColKey]*keys.BacklinkInfo {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	result := make(map[keys.ColKey]*keys.BacklinkInfo)
	if tableBacklinks, exists := bm.backlinks[tableKey]; exists {
		for colKey, info := range tableBacklinks {
			result[colKey] = info
		}
	}
	
	return result
}

// UpdateBacklinks updates backlinks when a forward link is modified
func (bm *BacklinkManager) UpdateBacklinks(originTable keys.TableKey, originCol keys.ColKey, 
	oldLink, newLink *keys.ObjLink) error {
	
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	// Remove old backlink if it exists
	if oldLink != nil && !oldLink.IsNull() {
		if err := bm.removeBacklinkEntry(oldLink.TableKey, oldLink.ObjKey, originTable, originCol); err != nil {
			return fmt.Errorf("failed to remove old backlink: %w", err)
		}
	}
	
	// Add new backlink if it exists
	if newLink != nil && !newLink.IsNull() {
		if err := bm.addBacklinkEntry(newLink.TableKey, newLink.ObjKey, originTable, originCol); err != nil {
			return fmt.Errorf("failed to add new backlink: %w", err)
		}
	}
	
	return nil
}

// addBacklinkEntry adds a backlink entry to the target object
func (bm *BacklinkManager) addBacklinkEntry(targetTable keys.TableKey, targetObj keys.ObjKey, 
	originTable keys.TableKey, originCol keys.ColKey) error {
	
	// Find the target table
	table, exists := bm.registry[targetTable]
	if !exists {
		return fmt.Errorf("target table %d not found in registry", targetTable)
	}
	
	// Find backlink columns for this origin
	for backlinkCol, info := range bm.backlinks[targetTable] {
		if info.OriginTable == originTable && info.OriginColumn == originCol {
			// Add the backlink entry to the target object
			return table.addBacklinkValue(targetObj, backlinkCol, originTable, targetObj)
		}
	}
	
	return nil
}

// removeBacklinkEntry removes a backlink entry from the target object
func (bm *BacklinkManager) removeBacklinkEntry(targetTable keys.TableKey, targetObj keys.ObjKey, 
	originTable keys.TableKey, originCol keys.ColKey) error {
	
	// Find the target table
	table, exists := bm.registry[targetTable]
	if !exists {
		return fmt.Errorf("target table %d not found in registry", targetTable)
	}
	
	// Find backlink columns for this origin
	for backlinkCol, info := range bm.backlinks[targetTable] {
		if info.OriginTable == originTable && info.OriginColumn == originCol {
			// Remove the backlink entry from the target object
			return table.removeBacklinkValue(targetObj, backlinkCol, originTable, targetObj)
		}
	}
	
	return nil
}

// ValidateBacklinks validates all backlink relationships
func (bm *BacklinkManager) ValidateBacklinks() error {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	for targetTable, tableBacklinks := range bm.backlinks {
		// Check if target table exists
		if _, exists := bm.registry[targetTable]; !exists {
			return fmt.Errorf("backlink target table %d not found in registry", targetTable)
		}
		
		for backlinkCol, info := range tableBacklinks {
			// Check if origin table exists
			if _, exists := bm.registry[info.OriginTable]; !exists {
				return fmt.Errorf("backlink origin table %d not found in registry", info.OriginTable)
			}
			
			// Validate backlink info
			if !info.IsValid() {
				return fmt.Errorf("invalid backlink info for table %d, column %d", targetTable, backlinkCol)
			}
		}
	}
	
	return nil
}

// GetBacklinkCount returns the total number of backlink relationships
func (bm *BacklinkManager) GetBacklinkCount() int {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	count := 0
	for _, tableBacklinks := range bm.backlinks {
		count += len(tableBacklinks)
	}
	
	return count
}

// Helper methods for table to manage backlink values

// addBacklinkValue adds a backlink value to an object (called by table)
func (t *Table) addBacklinkValue(objKey keys.ObjKey, backlinkCol keys.ColKey, 
	originTable keys.TableKey, originObj keys.ObjKey) error {
	
	// This would integrate with the cluster system to add backlink values
	// For now, we'll implement a basic version
	
	// Find the object in the cluster tree
	cluster, index := t.clusterTree.FindObject(objKey)
	if cluster == nil || index < 0 {
		return fmt.Errorf("object %d not found in table", objKey)
	}
	
	// Create the backlink reference
	backlinkRef := keys.NewObjLink(originTable, originObj)
	
	// Add to the backlink column (this would be implemented in cluster system)
	return cluster.AddBacklinkValue(backlinkCol, index, backlinkRef)
}

// removeBacklinkValue removes a backlink value from an object (called by table)
func (t *Table) removeBacklinkValue(objKey keys.ObjKey, backlinkCol keys.ColKey, 
	originTable keys.TableKey, originObj keys.ObjKey) error {
	
	// Find the object in the cluster tree
	cluster, index := t.clusterTree.FindObject(objKey)
	if cluster == nil || index < 0 {
		return fmt.Errorf("object %d not found in table", objKey)
	}
	
	// Create the backlink reference to remove
	backlinkRef := keys.NewObjLink(originTable, originObj)
	
	// Remove from the backlink column (this would be implemented in cluster system)
	return cluster.RemoveBacklinkValue(backlinkCol, index, backlinkRef)
}