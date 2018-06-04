package vars

type (
	IndexType int
)

const (
	StringIndex IndexType = iota
	IntIndex
	FloatIndex
	TimeIndex
	BytesIndex
)
