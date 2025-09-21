package keys

import (
	"fmt"
)

// TypedLink represents a typed link to an object in another table
// Similar to ObjLink but includes type information for the target
type TypedLink struct {
	TableKey TableKey // Target table key
	ObjKey   ObjKey   // Target object key
	TypeInfo DataType // Type information for the linked object
}

// NewTypedLink creates a new typed object link
func NewTypedLink(tableKey TableKey, objKey ObjKey, typeInfo DataType) TypedLink {
	return TypedLink{
		TableKey: tableKey,
		ObjKey:   objKey,
		TypeInfo: typeInfo,
	}
}

// IsNull returns true if the typed link is null (unresolved)
func (tl TypedLink) IsNull() bool {
	return tl.TableKey == 0 || tl.ObjKey == 0
}

// String returns a string representation of the typed link
func (tl TypedLink) String() string {
	if tl.IsNull() {
		return "TypedLink(null)"
	}
	return fmt.Sprintf("TypedLink(table:%d, obj:%d, type:%s)", tl.TableKey, tl.ObjKey, tl.TypeInfo.String())
}

// Encode encodes the TypedLink to bytes
func (tl TypedLink) Encode() []byte {
	buf := make([]byte, 13) // 4 bytes for TableKey + 8 bytes for ObjKey + 1 byte for TypeInfo

	// Encode TableKey (4 bytes)
	tableBytes := TableKey(tl.TableKey).Encode()
	copy(buf[0:4], tableBytes)

	// Encode ObjKey (8 bytes)
	objBytes := tl.ObjKey.Encode()
	copy(buf[4:12], objBytes)

	// Encode TypeInfo (1 byte)
	buf[12] = byte(tl.TypeInfo)

	return buf
}

// DecodeTypedLink decodes bytes to a TypedLink
func DecodeTypedLink(data []byte) (TypedLink, error) {
	if len(data) < 13 {
		return TypedLink{}, fmt.Errorf("insufficient data for TypedLink: need 13 bytes, got %d", len(data))
	}

	// Decode TableKey
	tableKey, err := DecodeTableKey(data[0:4])
	if err != nil {
		return TypedLink{}, fmt.Errorf("failed to decode TableKey: %w", err)
	}

	// Decode ObjKey
	objKey, err := DecodeObjKey(data[4:12])
	if err != nil {
		return TypedLink{}, fmt.Errorf("failed to decode ObjKey: %w", err)
	}

	// Decode TypeInfo
	typeInfo := DataType(data[12])

	return TypedLink{
		TableKey: tableKey,
		ObjKey:   objKey,
		TypeInfo: typeInfo,
	}, nil
}

// TypedLinkSet represents a set of typed links for list/set columns
type TypedLinkSet struct {
	Links []TypedLink
}

// NewTypedLinkSet creates a new typed link set
func NewTypedLinkSet() *TypedLinkSet {
	return &TypedLinkSet{
		Links: make([]TypedLink, 0),
	}
}

// Add adds a typed link to the set
func (tls *TypedLinkSet) Add(link TypedLink) {
	tls.Links = append(tls.Links, link)
}

// Remove removes a typed link from the set by index
func (tls *TypedLinkSet) Remove(index int) error {
	if index < 0 || index >= len(tls.Links) {
		return fmt.Errorf("index out of bounds: %d", index)
	}

	tls.Links = append(tls.Links[:index], tls.Links[index+1:]...)
	return nil
}

// RemoveTypedLink removes a specific typed link from the set
func (tls *TypedLinkSet) RemoveTypedLink(link TypedLink) bool {
	for i, l := range tls.Links {
		if l.TableKey == link.TableKey && l.ObjKey == link.ObjKey && l.TypeInfo == link.TypeInfo {
			tls.Remove(i)
			return true
		}
	}
	return false
}

// Contains checks if the set contains a specific typed link
func (tls *TypedLinkSet) Contains(link TypedLink) bool {
	for _, l := range tls.Links {
		if l.TableKey == link.TableKey && l.ObjKey == link.ObjKey && l.TypeInfo == link.TypeInfo {
			return true
		}
	}
	return false
}

// ContainsTarget checks if the set contains a link to a specific target (ignoring type)
func (tls *TypedLinkSet) ContainsTarget(tableKey TableKey, objKey ObjKey) bool {
	for _, l := range tls.Links {
		if l.TableKey == tableKey && l.ObjKey == objKey {
			return true
		}
	}
	return false
}

// Size returns the number of typed links in the set
func (tls *TypedLinkSet) Size() int {
	return len(tls.Links)
}

// Clear removes all typed links from the set
func (tls *TypedLinkSet) Clear() {
	tls.Links = tls.Links[:0]
}

// GetLinks returns a copy of all typed links
func (tls *TypedLinkSet) GetLinks() []TypedLink {
	result := make([]TypedLink, len(tls.Links))
	copy(result, tls.Links)
	return result
}

// GetLinksByType returns all links of a specific type
func (tls *TypedLinkSet) GetLinksByType(typeInfo DataType) []TypedLink {
	var result []TypedLink
	for _, link := range tls.Links {
		if link.TypeInfo == typeInfo {
			result = append(result, link)
		}
	}
	return result
}

// Encode encodes the TypedLinkSet to bytes
func (tls *TypedLinkSet) Encode() []byte {
	if len(tls.Links) == 0 {
		return []byte{0, 0, 0, 0} // 4 bytes for count = 0
	}

	// 4 bytes for count + 13 bytes per typed link
	buf := make([]byte, 4+len(tls.Links)*13)

	// Encode count
	count := uint32(len(tls.Links))
	buf[0] = byte(count)
	buf[1] = byte(count >> 8)
	buf[2] = byte(count >> 16)
	buf[3] = byte(count >> 24)

	// Encode each typed link
	offset := 4
	for _, link := range tls.Links {
		linkBytes := link.Encode()
		copy(buf[offset:offset+13], linkBytes)
		offset += 13
	}

	return buf
}

// DecodeTypedLinkSet decodes bytes to a TypedLinkSet
func DecodeTypedLinkSet(data []byte) (*TypedLinkSet, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("insufficient data for TypedLinkSet: need at least 4 bytes, got %d", len(data))
	}

	// Decode count
	count := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24

	expectedSize := 4 + int(count)*13
	if len(data) < expectedSize {
		return nil, fmt.Errorf("insufficient data for TypedLinkSet: need %d bytes, got %d", expectedSize, len(data))
	}

	tls := NewTypedLinkSet()

	// Decode each typed link
	offset := 4
	for i := uint32(0); i < count; i++ {
		link, err := DecodeTypedLink(data[offset : offset+13])
		if err != nil {
			return nil, fmt.Errorf("failed to decode typed link %d: %w", i, err)
		}
		tls.Add(link)
		offset += 13
	}

	return tls, nil
}