package vars

type (
	// IndexType defines what kind of feeld the index is scanning
	IndexType int
)

// Those define the deffrent type of indexes
const (
	StringIndex IndexType = iota
	IntIndex
	TimeIndex
)

// TypeName return the name of the type as a string
func (it IndexType) TypeName() string {
	switch it {
	case StringIndex:
		return "StringIndex"
	case IntIndex:
		return "IntIndex"
	case TimeIndex:
		return "TimeIndex"
	default:
		return ""
	}
}
