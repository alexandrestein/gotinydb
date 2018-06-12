package gotinydb

import (
	"reflect"
	"testing"
	"time"
)

func TestAction_ValueToCompareAsBytes(t *testing.T) {
	type fields struct {
		compareToValue interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			"String lower",
			fields{"string"},
			[]byte("string"),
		}, {
			"String upper",
			fields{"String"},
			[]byte("string"),
		}, {
			"int -1",
			fields{-1},
			[]byte{127, 255, 255, 255, 255, 255, 255, 255},
		}, {
			"int 0",
			fields{0},
			[]byte{128, 0, 0, 0, 0, 0, 0, 0},
		}, {
			"int 1",
			fields{1},
			[]byte{128, 0, 0, 0, 0, 0, 0, 1},
		}, {
			"uint 0",
			fields{uint(0)},
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			"uint 1",
			fields{uint(1)},
			[]byte{0, 0, 0, 0, 0, 0, 0, 1},
		}, {
			"float -1.5",
			fields{-1.5},
			[]byte{127, 255, 255, 255, 255, 255, 255, 255},
		}, {
			"float 0",
			fields{0.0},
			[]byte{128, 0, 0, 0, 0, 0, 0, 0},
		}, {
			"float 1.5",
			fields{1.5},
			[]byte{128, 0, 0, 0, 0, 0, 0, 1},
		}, {
			"time",
			fields{time.Time{}},
			[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255},
		}, {
			"bytes",
			fields{[]byte("OK")},
			[]byte{79, 75},
		}, {
			"nil",
			fields{nil},
			[]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Action{
				compareToValue: tt.fields.compareToValue,
			}
			if got := a.ValueToCompareAsBytes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Action.ValueToCompareAsBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
