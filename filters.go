package gotinydb

import (
	"time"
)

// NewEqualFilter builds a Filter pointer for equal query
func NewEqualFilter(value interface{}, s ...string) *Filter {
	ret := &Filter{
		operator: equal,
	}
	ret.compareTo(value)
	ret.setSelector(s...)
	return ret
}

// NewEqualAndGreaterFilter builds a Filter pointer for greater query
func NewEqualAndGreaterFilter(value interface{}, s ...string) *Filter {
	ret := &Filter{
		operator: greater,
	}
	ret.compareTo(value)
	ret.setSelector(s...)
	return ret
}

// NewEqualAndLessFilter builds a Filter pointer for less query
func NewEqualAndLessFilter(value interface{}, s ...string) *Filter {
	ret := &Filter{
		operator: less,
	}
	ret.compareTo(value)
	ret.setSelector(s...)
	return ret
}

// NewEqualAndBetweenFilter builds a Filter pointer for between query
func NewEqualAndBetweenFilter(from, to interface{}, s ...string) *Filter {
	ret := &Filter{
		operator: between,
	}
	ret.compareTo(from).compareTo(to)
	ret.setSelector(s...)
	return ret
}

// NewFieldExistsFilter builds a Filter pointer for field exists
func NewFieldExistsFilter(s ...string) *Filter {
	ret := &Filter{
		operator: exists,
	}
	ret.setSelector(s...)
	return ret
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

// compareTo defines the value you want to compare to
func (f *Filter) compareTo(val interface{}) *Filter {
	// Build the value if possible
	filterValuePointer, parseErr := newfilterValue(val)
	// If any error the value is not added
	if parseErr != nil {
		return f
	}

	// If the slice is nil or if the filter is not a between filter
	// the filter list has only one element
	if f.values == nil || f.operator != between {
		f.values = []*filterValue{filterValuePointer}
		return f
	}

	// Add the second value if it's between filter
	f.values = append(f.values, filterValuePointer)
	return f
}

// getType returns the type of the filter given at the initialization
func (f *Filter) getType() filterOperator {
	return f.operator
}

// setSelector defines the configurable limit of IDs.
func (f *Filter) setSelector(s ...string) *Filter {
	f.selector = s
	f.selectorHash = buildSelectorHash(s)
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
