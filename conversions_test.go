package gotinydb

import (
	"bytes"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestStringConversion(t *testing.T) {
	if _, err := stringToBytes("string to convert"); err != nil {
		t.Error(err)
		return
	}

	if _, err := stringToBytes(time.Now()); err == nil {
		t.Error(err)
		return
	}
}

func TestIntConversion(t *testing.T) {
	if _, err := intToBytes(31497863415); err != nil {
		t.Error(err)
		return
	}
	if _, err := intToBytes(int8(-117)); err != nil {
		t.Error(err)
		return
	}
	if _, err := intToBytes(int16(3847)); err != nil {
		t.Error(err)
		return
	}
	if _, err := intToBytes(int32(-7842245)); err != nil {
		t.Error(err)
		return
	}
	if _, err := intToBytes(int64(22416315751)); err != nil {
		t.Error(err)
		return
	}
}

func TestUIntConversion(t *testing.T) {
	if _, err := uintToBytes(uint(31497863415)); err != nil {
		t.Error(err)
		return
	}
	if _, err := uintToBytes(uint8(117)); err != nil {
		t.Error(err)
		return
	}
	if _, err := uintToBytes(uint16(3847)); err != nil {
		t.Error(err)
		return
	}
	if _, err := uintToBytes(uint32(7842245)); err != nil {
		t.Error(err)
		return
	}
	if _, err := uintToBytes(uint64(22416315751)); err != nil {
		t.Error(err)
		return
	}

	if _, err := uintToBytes(math.MaxInt64); err == nil {
		t.Error("this must return an error")
		return
	}
}

func TestIntOrdering(t *testing.T) {
	neg, _ := intToBytes(-1)
	null, _ := intToBytes(0)
	pos, _ := intToBytes(1)

	if !reflect.DeepEqual(neg, []byte{127, 255, 255, 255, 255, 255, 255, 255}) {
		t.Errorf("negative values is not what is expected: \n%v\n%v", neg, []byte{127, 255, 255, 255, 255, 255, 255, 255})
	} else if !reflect.DeepEqual(null, []byte{128, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("null values is not what is expected: \n%v\n%v", null, []byte{128, 0, 0, 0, 0, 0, 0, 0})
	} else if !reflect.DeepEqual(pos, []byte{128, 0, 0, 0, 0, 0, 0, 1}) {
		t.Errorf("positive values is not what is expected: \n%v\n%v", pos, []byte{128, 0, 0, 0, 0, 0, 0, 1})
	}

	if bytes.Compare(neg, null) >= 0 {
		t.Error("negative values are not smaller than null", neg, null)
	} else if bytes.Compare(null, pos) >= 0 {
		t.Error("null values are not smaller than positive", null, pos)
	} else if bytes.Compare(neg, pos) >= 0 {
		t.Error("negative values are not smaller than positive", neg, pos)
	}

	neg, _ = intToBytes(int64(math.MinInt64))
	null, _ = intToBytes(int64(0))
	pos, _ = intToBytes(int64(math.MaxInt64))

	if !reflect.DeepEqual(neg, []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("negative values is not what is expected: \n%v\n%v", neg, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	} else if !reflect.DeepEqual(null, []byte{128, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("null values is not what is expected: \n%v\n%v", null, []byte{128, 0, 0, 0, 0, 0, 0, 0})
	} else if !reflect.DeepEqual(pos, []byte{255, 255, 255, 255, 255, 255, 255, 255}) {
		t.Errorf("positive values is not what is expected: \n%v\n%v", pos, []byte{255, 255, 255, 255, 255, 255, 255, 255})
	}

	if bytes.Compare(neg, null) >= 0 {
		t.Error("negative values are not smaller than null", neg, null)
	} else if bytes.Compare(null, pos) >= 0 {
		t.Error("null values are not smaller than positive", null, pos)
	} else if bytes.Compare(neg, pos) >= 0 {
		t.Error("negative values are not smaller than positive", neg, pos)
	}

	if _, err := intToBytes(time.Now()); err == nil {
		t.Error(err)
		return
	}
}

func TestTimeConversion(t *testing.T) {
	if _, err := timeToBytes(time.Now()); err != nil {
		t.Error(err)
		return
	}

	if _, err := timeToBytes("is it time?"); err == nil {
		t.Error(err)
		return
	}
}
