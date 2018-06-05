package vars

import (
	"encoding/binary"
	"math/big"
	"time"
)

// StringToBytes converter from a string to bytes slice.
// If an error is returned it's has the form of ErrWrongType
func StringToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(string)
	if !ok {
		return nil, ErrWrongType
	}

	return []byte(typedInput), nil
}

// IntToBytes converter from a int or uint of any size (int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64)
// to bytes slice. If an error is returned it's has the form of ErrWrongType
func IntToBytes(input interface{}) ([]byte, error) {
	typedValue := uint64(0)
	switch input.(type) {
	case int:
		typedValue = uint64(input.(int))
	case int8:
		typedValue = uint64(input.(int8))
	case int16:
		typedValue = uint64(input.(int16))
	case int32:
		typedValue = uint64(input.(int32))
	case int64:
		typedValue = uint64(input.(int64))
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
	binary.LittleEndian.PutUint64(bs, typedValue)
	return bs, nil
}

// FloatToBytes converter from a float32 or float64 to bytes slice.
// If an error is returned it's has the form of ErrWrongType
func FloatToBytes(input interface{}) ([]byte, error) {
	var bigFloat *big.Float
	switch input.(type) {
	case float32:
		typedValue := float64(input.(float32))
		bigFloat = big.NewFloat(typedValue)
	case float64:
		typedValue := input.(float64)
		bigFloat = big.NewFloat(typedValue)
	default:
		return nil, ErrWrongType
	}

	uint64Val, _ := bigFloat.Uint64()

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64Val)
	return bs, nil
}

// TimeToBytes converter from a time struct to bytes slice.
// If an error is returned it's has the form of ErrWrongType
func TimeToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(time.Time)
	if !ok {
		return nil, ErrWrongType
	}

	return typedInput.MarshalBinary()
}
