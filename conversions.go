package gotinydb

import (
	"encoding/binary"
	"math"
	"time"
)

// stringToBytes converter from a string to bytes slice.
// If an error is returned it's has the form of ErrWrongType
func stringToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(string)
	if !ok {
		return nil, ErrWrongType
	}

	return []byte(typedInput), nil
}

// intToBytes converter from a int or uint of any size (int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64)
// to bytes slice. If an error is returned it's has the form of ErrWrongType
func intToBytes(input interface{}) ([]byte, error) {
	typedValue := uint64(0)
	switch input.(type) {
	case int, int8, int16, int32, int64:
		typedValue = convertIntToAbsoluteUint(input)
	default:
		return nil, ErrWrongType
	}

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, typedValue)
	return bs, nil
}
func uintToBytes(input interface{}) ([]byte, error) {
	typedValue := uint64(0)
	switch input.(type) {
	case uint:
		typedValue = uint64(input.(uint))
	case uint8:
		typedValue = uint64(input.(uint8))
	case uint16:
		typedValue = uint64(input.(uint16))
	case uint32:
		typedValue = uint64(input.(uint32))
	case uint64:
		typedValue = input.(uint64)
	default:
		return nil, ErrWrongType
	}

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, typedValue)
	return bs, nil
}

func convertIntToAbsoluteUint(input interface{}) (ret uint64) {
	typedValue := int64(0)

	switch input.(type) {
	case int:
		typedValue = int64(input.(int))
	case int8:
		typedValue = int64(input.(int8))
	case int16:
		typedValue = int64(input.(int16))
	case int32:
		typedValue = int64(input.(int32))
	case int64:
		typedValue = int64(input.(int64))
	}

	ret = uint64(typedValue) + (math.MaxUint64 / 2) + 1

	return ret
}

// timeToBytes converter from a time struct to bytes slice.
// If an error is returned it's has the form of ErrWrongType
func timeToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(time.Time)
	if !ok {
		return nil, ErrWrongType
	}

	return typedInput.MarshalBinary()
}
