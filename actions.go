package gotinydb

import (
	"time"

	"github.com/alexandrestein/gotinydb/vars"
)

// NewAction returns a new Action pointer with the given ActionType
func NewAction(t ActionType) *Action {
	return &Action{
		operation: t,
	}
}

// CompareTo defines the value you want to compare to
func (a *Action) CompareTo(val interface{}) *Action {
	a.compareToValue = val
	return a
}

func (a *Action) ValueToCompareAsBytes() []byte {
	switch a.compareToValue.(type) {
	case string:
		bytes, _ := vars.StringToBytes(a.compareToValue)
		return bytes
	case int, int8, int32, int64, uint, uint8, uint32, uint64:
		bytes, _ := vars.IntToBytes(a.compareToValue)
		return bytes
	case float32, float64:
		bytes, _ := vars.FloatToBytes(a.compareToValue)
		return bytes
	case time.Time:
		bytes, _ := vars.TimeToBytes(a.compareToValue)
		return bytes
	case []byte:
		return a.compareToValue.([]byte)
	}
	return []byte{}
}

// GetType returns the type of the action given at the initialisation
func (a *Action) GetType() ActionType {
	return a.operation
}

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (a *Action) EqualWanted() *Action {
	a.equal = true
	return a
}

// SetSelector defines the configurable limit of IDs.
func (a *Action) SetSelector(s []string) *Action {
	a.selector = s
	return a
}
