package keys

// ColumnAttr represents column attributes as bit flags
type ColumnAttr int

const (
	ColAttrNone            ColumnAttr = 0
	ColAttrIndexed         ColumnAttr = 1
	ColAttrUnique          ColumnAttr = 2             // Requires ColAttrIndexed
	ColAttrReserved        ColumnAttr = 4             // Reserved for future use
	ColAttrStrongLinks     ColumnAttr = 8             // Strong links (not weak)
	ColAttrNullable        ColumnAttr = 16            // Elements can be null
	ColAttrList            ColumnAttr = 32            // Each element is a list
	ColAttrDictionary      ColumnAttr = 64            // Each element is a dictionary
	ColAttrSet             ColumnAttr = 128           // Each element is a set
	ColAttrFullTextIndexed ColumnAttr = 256           // Full-text indexed
	ColAttrCollection      ColumnAttr = 128 + 64 + 32 // Either list, dictionary, or set
)

// ColumnAttrMask manages column attributes using bit operations
type ColumnAttrMask struct {
	value int
}

// NewColumnAttrMask creates a new ColumnAttrMask with no attributes set
func NewColumnAttrMask() ColumnAttrMask {
	return ColumnAttrMask{value: 0}
}

// NewColumnAttrMaskWithValue creates a ColumnAttrMask from an integer value
func NewColumnAttrMaskWithValue(val int) ColumnAttrMask {
	return ColumnAttrMask{value: val}
}

// Test checks if a specific attribute is set
func (cam ColumnAttrMask) Test(attr ColumnAttr) bool {
	return (cam.value & int(attr)) != 0
}

// Set adds an attribute to the mask
func (cam *ColumnAttrMask) Set(attr ColumnAttr) {
	cam.value |= int(attr)
}

// Reset removes an attribute from the mask
func (cam *ColumnAttrMask) Reset(attr ColumnAttr) {
	cam.value &= ^int(attr)
}

// Value returns the internal integer value
func (cam ColumnAttrMask) Value() int {
	return cam.value
}

// Equals checks if two ColumnAttrMask instances are equal
func (cam ColumnAttrMask) Equals(other ColumnAttrMask) bool {
	return cam.value == other.value
}

// NotEquals checks if two ColumnAttrMask instances are not equal
func (cam ColumnAttrMask) NotEquals(other ColumnAttrMask) bool {
	return cam.value != other.value
}

// IsNullable checks if the nullable attribute is set
func (cam ColumnAttrMask) IsNullable() bool {
	return cam.Test(ColAttrNullable)
}

// IsList checks if the list attribute is set
func (cam ColumnAttrMask) IsList() bool {
	return cam.Test(ColAttrList)
}

// IsSet checks if the set attribute is set
func (cam ColumnAttrMask) IsSet() bool {
	return cam.Test(ColAttrSet)
}

// IsDictionary checks if the dictionary attribute is set
func (cam ColumnAttrMask) IsDictionary() bool {
	return cam.Test(ColAttrDictionary)
}

// IsCollection checks if any collection attribute is set
func (cam ColumnAttrMask) IsCollection() bool {
	return cam.Test(ColAttrCollection)
}

// IsIndexed checks if the indexed attribute is set
func (cam ColumnAttrMask) IsIndexed() bool {
	return cam.Test(ColAttrIndexed)
}

// IsUnique checks if the unique attribute is set
func (cam ColumnAttrMask) IsUnique() bool {
	return cam.Test(ColAttrUnique)
}

// IsFullTextIndexed checks if the full-text indexed attribute is set
func (cam ColumnAttrMask) IsFullTextIndexed() bool {
	return cam.Test(ColAttrFullTextIndexed)
}

// HasStrongLinks checks if the strong links attribute is set
func (cam ColumnAttrMask) HasStrongLinks() bool {
	return cam.Test(ColAttrStrongLinks)
}

// String returns a string representation of the ColumnAttrMask
func (cam ColumnAttrMask) String() string {
	if cam.value == 0 {
		return "None"
	}

	var attrs []string
	if cam.IsIndexed() {
		attrs = append(attrs, "Indexed")
	}
	if cam.IsUnique() {
		attrs = append(attrs, "Unique")
	}
	if cam.HasStrongLinks() {
		attrs = append(attrs, "StrongLinks")
	}
	if cam.IsNullable() {
		attrs = append(attrs, "Nullable")
	}
	if cam.IsList() {
		attrs = append(attrs, "List")
	}
	if cam.IsDictionary() {
		attrs = append(attrs, "Dictionary")
	}
	if cam.IsSet() {
		attrs = append(attrs, "Set")
	}
	if cam.IsFullTextIndexed() {
		attrs = append(attrs, "FullTextIndexed")
	}

	result := ""
	for i, attr := range attrs {
		if i > 0 {
			result += "|"
		}
		result += attr
	}
	return result
}
