package gotinydb

// // stringToBytes converter from a string to bytes slice.
// // If an error is returned it's has the form of ErrWrongType
// func stringToBytes(input interface{}) ([]byte, error) {
// 	typedInput, ok := input.(string)
// 	if !ok {
// 		return nil, ErrWrongType
// 	}

// 	return []byte(typedInput), nil
// }

// // intToBytes converter from a int or uint of any size (int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64)
// // to bytes slice. If an error is returned it's has the form of ErrWrongType
// func intToBytes(input interface{}) ([]byte, error) {
// 	typedValue := uint64(0)
// 	switch val := input.(type) {
// 	case int, int8, int16, int32, int64:
// 		typedValue = convertIntToAbsoluteUint(val)
// 	case json.Number:
// 		asInt64, err := val.Int64()
// 		if err != nil {
// 			return nil, err
// 		}
// 		typedValue = convertIntToAbsoluteUint(asInt64)
// 	default:
// 		return nil, ErrWrongType
// 	}

// 	bs := make([]byte, 8)
// 	binary.BigEndian.PutUint64(bs, typedValue)
// 	return bs, nil
// }
// func uintToBytes(input interface{}) ([]byte, error) {
// 	typedValue := uint64(0)
// 	switch val := input.(type) {
// 	case uint:
// 		typedValue = uint64(val)
// 	case uint8:
// 		typedValue = uint64(val)
// 	case uint16:
// 		typedValue = uint64(val)
// 	case uint32:
// 		typedValue = uint64(val)
// 	case uint64:
// 		typedValue = val
// 	case json.Number:
// 		var err error
// 		typedValue, err = strconv.ParseUint(val.String(), 10, 64)
// 		if err != nil {
// 			return nil, err
// 		}
// 	default:
// 		return nil, ErrWrongType
// 	}

// 	bs := make([]byte, 8)
// 	binary.BigEndian.PutUint64(bs, typedValue)
// 	return bs, nil
// }

// func convertIntToAbsoluteUint(input interface{}) (ret uint64) {
// 	typedValue := int64(0)

// 	switch val := input.(type) {
// 	case int:
// 		typedValue = int64(val)
// 	case int8:
// 		typedValue = int64(val)
// 	case int16:
// 		typedValue = int64(val)
// 	case int32:
// 		typedValue = int64(val)
// 	case int64:
// 		typedValue = val
// 	}

// 	ret = uint64(typedValue) + (math.MaxUint64 / 2) + 1

// 	return ret
// }

// // timeToBytes converter from a time struct to bytes slice.
// // If an error is returned it's has the form of ErrWrongType
// func timeToBytes(input interface{}) ([]byte, error) {
// 	var typedInput time.Time
// 	switch val := input.(type) {
// 	case time.Time:
// 		typedInput = val
// 	case string:
// 		var err error
// 		typedInput, err = time.Parse(time.RFC3339, val)
// 		if err != nil {
// 			return nil, err
// 		}
// 	default:
// 		return nil, ErrWrongType
// 	}
// 	// typedInput, ok := input.(time.Time)
// 	// if !ok {
// 	// }

// 	return typedInput.MarshalBinary()
// }
