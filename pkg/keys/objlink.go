package keys

import (
	"fmt"
)

// ObjLink represents a link to an object in another table
type ObjLink struct {
	TableKey TableKey // Target table key
	ObjKey   ObjKey   // Target object key
}

// NewObjLink creates a new object link
func NewObjLink(tableKey TableKey, objKey ObjKey) ObjLink {
	return ObjLink{
		TableKey: tableKey,
		ObjKey:   objKey,
	}
}

// IsNull returns true if the link is null (unresolved)
func (ol ObjLink) IsNull() bool {
	return ol.TableKey == 0 || ol.ObjKey == 0
}

// String returns a string representation of the object link
func (ol ObjLink) String() string {
	if ol.IsNull() {
		return "ObjLink(null)"
	}
	return fmt.Sprintf("ObjLink(table:%d, obj:%d)", ol.TableKey, ol.ObjKey)
}

// Encode encodes the ObjLink to bytes
func (ol ObjLink) Encode() []byte {
	buf := make([]byte, 12) // 4 bytes for TableKey + 8 bytes for ObjKey

	// Encode TableKey (4 bytes)
	tableBytes := TableKey(ol.TableKey).Encode()
	copy(buf[0:4], tableBytes)

	// Encode ObjKey (8 bytes)
	objBytes := ol.ObjKey.Encode()
	copy(buf[4:12], objBytes)

	return buf
}

// DecodeObjLink decodes bytes to an ObjLink
func DecodeObjLink(data []byte) (ObjLink, error) {
	if len(data) < 12 {
		return ObjLink{}, fmt.Errorf("insufficient data for ObjLink: need 12 bytes, got %d", len(data))
	}

	// Decode TableKey
	tableKey, err := DecodeTableKey(data[0:4])
	if err != nil {
		return ObjLink{}, fmt.Errorf("failed to decode TableKey: %w", err)
	}

	// Decode ObjKey
	objKey, err := DecodeObjKey(data[4:12])
	if err != nil {
		return ObjLink{}, fmt.Errorf("failed to decode ObjKey: %w", err)
	}

	return ObjLink{
		TableKey: tableKey,
		ObjKey:   objKey,
	}, nil
}

// BacklinkInfo represents backlink metadata for a column
type BacklinkInfo struct {
	OriginTable  TableKey // Table that contains the forward link
	OriginColumn ColKey   // Column that contains the forward link
	TargetTable  TableKey // Table that is the target of the backlink
}

// NewBacklinkInfo creates new backlink information
func NewBacklinkInfo(originTable TableKey, originColumn ColKey, targetTable TableKey) BacklinkInfo {
	return BacklinkInfo{
		OriginTable:  originTable,
		OriginColumn: originColumn,
		TargetTable:  targetTable,
	}
}

// IsValid returns true if the backlink info is valid
func (bi BacklinkInfo) IsValid() bool {
	return bi.OriginTable != 0 && bi.OriginColumn != 0 && bi.TargetTable != 0
}

// String returns a string representation of the backlink info
func (bi BacklinkInfo) String() string {
	return fmt.Sprintf("BacklinkInfo(origin_table:%d, origin_col:%d, target_table:%d)",
		bi.OriginTable, bi.OriginColumn, bi.TargetTable)
}

// LinkSet represents a set of object links for list/set columns
type LinkSet struct {
	Links []ObjLink
}

// NewLinkSet creates a new link set
func NewLinkSet() *LinkSet {
	return &LinkSet{
		Links: make([]ObjLink, 0),
	}
}

// Add adds a link to the set
func (ls *LinkSet) Add(link ObjLink) {
	ls.Links = append(ls.Links, link)
}

// Remove removes a link from the set by index
func (ls *LinkSet) Remove(index int) error {
	if index < 0 || index >= len(ls.Links) {
		return fmt.Errorf("index out of bounds: %d", index)
	}

	ls.Links = append(ls.Links[:index], ls.Links[index+1:]...)
	return nil
}

// RemoveLink removes a specific link from the set
func (ls *LinkSet) RemoveLink(link ObjLink) bool {
	for i, l := range ls.Links {
		if l.TableKey == link.TableKey && l.ObjKey == link.ObjKey {
			ls.Remove(i)
			return true
		}
	}
	return false
}

// Contains checks if the set contains a specific link
func (ls *LinkSet) Contains(link ObjLink) bool {
	for _, l := range ls.Links {
		if l.TableKey == link.TableKey && l.ObjKey == link.ObjKey {
			return true
		}
	}
	return false
}

// Size returns the number of links in the set
func (ls *LinkSet) Size() int {
	return len(ls.Links)
}

// Clear removes all links from the set
func (ls *LinkSet) Clear() {
	ls.Links = ls.Links[:0]
}

// GetLinks returns a copy of all links
func (ls *LinkSet) GetLinks() []ObjLink {
	result := make([]ObjLink, len(ls.Links))
	copy(result, ls.Links)
	return result
}

// Encode encodes the LinkSet to bytes
func (ls *LinkSet) Encode() []byte {
	if len(ls.Links) == 0 {
		return []byte{0, 0, 0, 0} // 4 bytes for count = 0
	}

	// 4 bytes for count + 12 bytes per link
	buf := make([]byte, 4+len(ls.Links)*12)

	// Encode count
	count := uint32(len(ls.Links))
	buf[0] = byte(count)
	buf[1] = byte(count >> 8)
	buf[2] = byte(count >> 16)
	buf[3] = byte(count >> 24)

	// Encode each link
	offset := 4
	for _, link := range ls.Links {
		linkBytes := link.Encode()
		copy(buf[offset:offset+12], linkBytes)
		offset += 12
	}

	return buf
}

// DecodeLinkSet decodes bytes to a LinkSet
func DecodeLinkSet(data []byte) (*LinkSet, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("insufficient data for LinkSet: need at least 4 bytes, got %d", len(data))
	}

	// Decode count
	count := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24

	expectedSize := 4 + int(count)*12
	if len(data) < expectedSize {
		return nil, fmt.Errorf("insufficient data for LinkSet: need %d bytes, got %d", expectedSize, len(data))
	}

	ls := NewLinkSet()

	// Decode each link
	offset := 4
	for i := uint32(0); i < count; i++ {
		link, err := DecodeObjLink(data[offset : offset+12])
		if err != nil {
			return nil, fmt.Errorf("failed to decode link %d: %w", i, err)
		}
		ls.Add(link)
		offset += 12
	}

	return ls, nil
}
