package gotinydb

import (
	"reflect"
	"testing"
	"time"
)

func TestAction_ValueToCompareAsBytes(t *testing.T) {
	tests := []struct {
		name        string
		filterValue interface{}
		want        []byte
	}{
		{
			"String lower",
			"string",
			[]byte("string"),
		}, {
			"String upper",
			"String",
			[]byte("String"),
		}, {
			"int -1",
			-1,
			[]byte{127, 255, 255, 255, 255, 255, 255, 255},
		}, {
			"int 0",
			0,
			[]byte{128, 0, 0, 0, 0, 0, 0, 0},
		}, {
			"int 1",
			1,
			[]byte{128, 0, 0, 0, 0, 0, 0, 1},
		}, {
			"uint 0",
			uint(0),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			"uint 1",
			uint(1),
			[]byte{0, 0, 0, 0, 0, 0, 0, 1},
		}, {
			"time",
			time.Time{},
			[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := new(filterBase)
			f.CompareTo(tt.filterValue)
			if got := f.values[0].Bytes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Action.ValueToCompareAsBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
