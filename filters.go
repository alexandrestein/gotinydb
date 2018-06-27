package gotinydb

import (
	"time"

	"github.com/alexandrestein/gotinydb/vars"
)

type (
	// Filter defines the way the query will be performed
	Filter struct {
		selector     []string
		selectorHash uint64
		operator     FilterOperator
		values       []*FilterValue
		equal        bool
	}

	// FilterValue defines the value we need to compare to
	FilterValue struct {
		Value interface{}
		Type  vars.IndexType
	}
)

// NewFilter returns a new Action pointer with the given FilterOperator
func NewFilter(t FilterOperator) *Filter {
	return &Filter{
		operator: t,
	}
}

// NewFilterValue build a new filter value to be used inside the filters
func NewFilterValue(value interface{}) (*FilterValue, error) {
	var t vars.IndexType
	switch value.(type) {
	case string:
		t = vars.StringIndex
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		t = vars.IntIndex
	case time.Time:
		t = vars.TimeIndex
	case []byte:
		t = vars.BytesIndex
	default:
		return nil, vars.ErrWrongType
	}

	filterValue := new(FilterValue)
	filterValue.Type = t
	filterValue.Value = value

	return filterValue, nil
}

// // MustNewFilterValue same as above but call is certain the type is OK
// func MustNewFilterValue(value interface{}) *FilterValue {
// 	v, _ := NewFilterValue(value)
// 	return v
// }

// CompareTo defines the value you want to compare to
func (f *Filter) CompareTo(val interface{}) *Filter {
	// Build the value if possible
	filterValue, parseErr := NewFilterValue(val)
	// If any error the value is not added
	if parseErr != nil {
		return f
	}

	// If the slice is nil or if the filter is not a between filter
	// the filter list has only one element
	if f.values == nil || f.operator != Between {
		f.values = []*FilterValue{filterValue}
		return f
	}

	// Limit the numbers of 2 filters
	if len(f.values) >= 2 {
		f.values[1] = filterValue
	}

	// Add the second value if it's between filter
	f.values = append(f.values, filterValue)
	return f
}

// ValueToCompareAsBytes returns the given value as bytes to make it easy to compare
func (f *Filter) ValueToCompareAsBytes(n int) []byte {
	if n >= len(f.values) {
		return []byte{}
	}
	return f.values[n].Bytes()
}

// GetType returns the type of the filter given at the initialization
func (f *Filter) GetType() FilterOperator {
	return f.operator
}

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (f *Filter) EqualWanted() *Filter {
	f.equal = true
	return f
}

// SetSelector defines the configurable limit of IDs.
func (f *Filter) SetSelector(s []string) *Filter {
	f.selector = s
	f.selectorHash = vars.BuildSelectorHash(s)
	return f
}

// Bytes returns the value as a slice of bytes
func (f *FilterValue) Bytes() []byte {
	var bytes []byte
	switch f.Type {
	case vars.StringIndex:
		bytes, _ = vars.StringToBytes(f.Value)
	case vars.IntIndex:
		bytes, _ = vars.IntToBytes(f.Value)
	case vars.TimeIndex:
		bytes, _ = vars.TimeToBytes(f.Value)
	case vars.BytesIndex:
		return f.Value.([]byte)
	}
	return bytes
}
