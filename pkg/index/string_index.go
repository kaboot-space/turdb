package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
)

// StringIndex implements string-based indexing similar to realm-core StringIndex
type StringIndex struct {
	targetColumn *cluster.ClusterColumnArray
	allocator    *core.Allocator
	entries      map[string][]keys.ObjKey // String value to object keys mapping
	mutex        sync.RWMutex
	isFulltext   bool
}

// NewStringIndex creates a new string index for the given column
func NewStringIndex(targetColumn *cluster.ClusterColumnArray, allocator *core.Allocator) *StringIndex {
	return &StringIndex{
		targetColumn: targetColumn,
		allocator:    allocator,
		entries:      make(map[string][]keys.ObjKey),
		isFulltext:   false, // TODO: implement tokenization check
	}
}

// TypeSupported checks if the data type is supported by string index
func (si *StringIndex) TypeSupported(dataType keys.DataType) bool {
	switch dataType {
	case keys.TypeInt, keys.TypeString, keys.TypeBool, 
		 keys.TypeTimestamp, keys.TypeObjectID, keys.TypeMixed, keys.TypeUUID:
		return true
	default:
		return false
	}
}

// IsEmpty returns true if the index is empty
func (si *StringIndex) IsEmpty() bool {
	si.mutex.RLock()
	defer si.mutex.RUnlock()
	return len(si.entries) == 0
}

// IsFulltextIndex returns true if this is a fulltext index
func (si *StringIndex) IsFulltextIndex() bool {
	return si.isFulltext
}

// Insert adds a new entry to the index
func (si *StringIndex) Insert(key keys.ObjKey, value interface{}) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	strValue := si.convertToString(value)
	if si.isFulltext {
		return si.insertFulltext(key, strValue)
	}
	return si.insertExact(key, strValue)
}

// Set updates an existing entry in the index
func (si *StringIndex) Set(key keys.ObjKey, newValue interface{}) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	// Remove old value first
	si.eraseInternal(key)
	
	// Insert new value
	strValue := si.convertToString(newValue)
	if si.isFulltext {
		return si.insertFulltext(key, strValue)
	}
	return si.insertExact(key, strValue)
}

// Erase removes an entry from the index
func (si *StringIndex) Erase(key keys.ObjKey) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()
	return si.eraseInternal(key)
}

// EraseString removes a specific string value for a key
func (si *StringIndex) EraseString(key keys.ObjKey, value string) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if objKeys, exists := si.entries[value]; exists {
		for i, objKey := range objKeys {
			if objKey == key {
				si.entries[value] = append(objKeys[:i], objKeys[i+1:]...)
				if len(si.entries[value]) == 0 {
					delete(si.entries, value)
				}
				break
			}
		}
	}
	return nil
}

// FindFirst finds the first object key matching the given value
func (si *StringIndex) FindFirst(value interface{}) (keys.ObjKey, error) {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	strValue := si.convertToString(value)
	if objKeys, exists := si.entries[strValue]; exists && len(objKeys) > 0 {
		return objKeys[0], nil
	}
	return keys.ObjKey(0), fmt.Errorf("value not found")
}

// FindAll finds all object keys matching the given value
func (si *StringIndex) FindAll(value interface{}) ([]keys.ObjKey, error) {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	strValue := si.convertToString(value)
	if objKeys, exists := si.entries[strValue]; exists {
		result := make([]keys.ObjKey, len(objKeys))
		copy(result, objKeys)
		return result, nil
	}
	return []keys.ObjKey{}, nil
}

// FindAllPrefix finds all object keys with values starting with the given prefix
func (si *StringIndex) FindAllPrefix(prefix string) ([]keys.ObjKey, error) {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	var result []keys.ObjKey
	for value, objKeys := range si.entries {
		if strings.HasPrefix(value, prefix) {
			result = append(result, objKeys...)
		}
	}
	
	// Sort results for consistent ordering
	sort.Slice(result, func(i, j int) bool {
		return uint64(result[i]) < uint64(result[j])
	})
	
	return result, nil
}

// Count returns the number of entries matching the given value
func (si *StringIndex) Count(value interface{}) (uint64, error) {
	si.mutex.RLock()
	defer si.mutex.RUnlock()

	strValue := si.convertToString(value)
	if objKeys, exists := si.entries[strValue]; exists {
		return uint64(len(objKeys)), nil
	}
	return 0, nil
}

// Clear removes all entries from the index
func (si *StringIndex) Clear() error {
	si.mutex.Lock()
	defer si.mutex.Unlock()
	si.entries = make(map[string][]keys.ObjKey)
	return nil
}

// Verify checks the integrity of the index (debug mode)
func (si *StringIndex) Verify() error {
	si.mutex.RLock()
	defer si.mutex.RUnlock()
	
	// Basic verification - check for duplicate keys in value lists
	for value, objKeys := range si.entries {
		keySet := make(map[keys.ObjKey]bool)
		for _, key := range objKeys {
			if keySet[key] {
				return fmt.Errorf("duplicate key %v found for value %s", key, value)
			}
			keySet[key] = true
		}
	}
	return nil
}

// insertExact inserts a value for exact matching
func (si *StringIndex) insertExact(key keys.ObjKey, value string) error {
	si.entries[value] = append(si.entries[value], key)
	return nil
}

// insertFulltext inserts a value for fulltext search (tokenized)
func (si *StringIndex) insertFulltext(key keys.ObjKey, value string) error {
	tokens := si.tokenize(value)
	for _, token := range tokens {
		si.entries[token] = append(si.entries[token], key)
	}
	return nil
}

// eraseInternal removes all entries for a given key
func (si *StringIndex) eraseInternal(key keys.ObjKey) error {
	for value, objKeys := range si.entries {
		for i, objKey := range objKeys {
			if objKey == key {
				si.entries[value] = append(objKeys[:i], objKeys[i+1:]...)
				if len(si.entries[value]) == 0 {
					delete(si.entries, value)
				}
				break
			}
		}
	}
	return nil
}

// convertToString converts various types to string representation
func (si *StringIndex) convertToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return fmt.Sprintf("%d", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// tokenize splits text into tokens for fulltext search
func (si *StringIndex) tokenize(text string) []string {
	// Simple tokenization - split by whitespace and punctuation
	words := strings.FieldsFunc(text, func(c rune) bool {
		return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
	})
	
	// Convert to lowercase and remove duplicates
	tokenSet := make(map[string]bool)
	for _, word := range words {
		if len(word) > 0 {
			tokenSet[strings.ToLower(word)] = true
		}
	}
	
	tokens := make([]string, 0, len(tokenSet))
	for token := range tokenSet {
		tokens = append(tokens, token)
	}
	
	return tokens
}