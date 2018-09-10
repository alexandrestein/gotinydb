package gotinydb

import (
	"time"
)

// NewEqualFilter builds a Filter interface for equal query
func NewEqualFilter(value interface{}, s ...string) Filter {
	ret := &filterBase{
		operator: Equal,
	}
	ret.CompareTo(value)
	ret.SetSelector(s...)
	return Filter(ret)
}

// NewGreaterFilter builds a Filter interface for greater query
func NewGreaterFilter(value interface{}, s ...string) Filter {
	ret := &filterBase{
		operator: Greater,
	}
	ret.CompareTo(value)
	ret.SetSelector(s...)
	return Filter(ret)
}

// NewLessFilter builds a Filter interface for less query
func NewLessFilter(value interface{}, s ...string) Filter {
	ret := &filterBase{
		operator: Less,
	}
	ret.CompareTo(value)
	ret.SetSelector(s...)
	return Filter(ret)
}

// NewBetweenFilter builds a Filter interface for between query
func NewBetweenFilter(from, to interface{}, s ...string) Filter {
	ret := &filterBase{
		operator: Between,
	}
	ret.CompareTo(from).CompareTo(to)
	ret.SetSelector(s...)
	return Filter(ret)
}

// newfilterValue build a new filter value to be used inside the filters
func newfilterValue(value interface{}) (*filterValue, error) {
	var t IndexType
	switch value.(type) {
	case string:
		t = StringIndex
	case int, int8, int16, int32, int64:
		t = IntIndex
	case uint, uint8, uint16, uint32, uint64:
		t = UIntIndex
	case time.Time:
		t = TimeIndex
	default:
		return nil, ErrWrongType
	}

	filterValue := new(filterValue)
	filterValue.Type = t
	filterValue.Value = value

	return filterValue, nil
}

// CompareTo defines the value you want to compare to
func (f *filterBase) CompareTo(val interface{}) *filterBase {
	// Build the value if possible
	filterValuePointer, parseErr := newfilterValue(val)
	// If any error the value is not added
	if parseErr != nil {
		return f
	}

	// If the slice is nil or if the filter is not a between filter
	// the filter list has only one element
	if f.values == nil || f.operator != Between {
		f.values = []*filterValue{filterValuePointer}
		return f
	}

	// Limit the numbers of 2 filters
	if len(f.values) >= 2 {
		f.values[1] = filterValuePointer
	}

	// Add the second value if it's between filter
	f.values = append(f.values, filterValuePointer)
	return f
}

// GetType returns the type of the filter given at the initialization
func (f *filterBase) GetType() FilterOperator {
	return f.operator
}

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (f *filterBase) EqualWanted() Filter {
	f.equal = true
	return f
}

// ExclusionFilter set the given Filter to be used as a cleaner filter.
// When IDs are retrieved by those filters the IDs will not be returned at response.
func (f *filterBase) ExclusionFilter() Filter {
	f.exclusion = true
	return f
}

// SetSelector defines the configurable limit of IDs.
func (f *filterBase) SetSelector(s ...string) *filterBase {
	f.selector = s
	f.selectorHash = buildSelectorHash(s)
	return f
}

func (f *filterBase) getFilterBase() *filterBase {
	return f
}

// Bytes returns the value as a slice of bytes
func (f *filterValue) Bytes() []byte {
	var bytes []byte
	switch f.Type {
	case StringIndex:
		bytes, _ = stringToBytes(f.Value)
	case IntIndex:
		bytes, _ = intToBytes(f.Value)
	case UIntIndex:
		bytes, _ = uintToBytes(f.Value)
	case TimeIndex:
		bytes, _ = timeToBytes(f.Value)
	default:
		return nil
	}
	return bytes
}
