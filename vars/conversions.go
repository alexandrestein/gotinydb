package vars

import (
	"encoding/binary"
	"math/big"
	"time"
)

func StringToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(string)
	if !ok {
		return nil, WrongType
	}

	return []byte(typedInput), nil
}

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
		return nil, WrongType
	}

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, typedValue)
	return bs, nil
}

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
		return nil, WrongType
	}

	uint64Val, _ := bigFloat.Uint64()

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64Val)
	return bs, nil
}

func TimeToBytes(input interface{}) ([]byte, error) {
	typedInput, ok := input.(time.Time)
	if !ok {
		return nil, WrongType
	}

	return typedInput.MarshalBinary()
}
