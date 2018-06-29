package gotinydb

import (
	"bytes"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestStringConversion(t *testing.T) {
	if _, err := StringToBytes("string to convert"); err != nil {
		t.Error(err)
		return
	}

	if _, err := StringToBytes(time.Now()); err == nil {
		t.Error(err)
		return
	}
}

func TestIntConversion(t *testing.T) {
	if _, err := IntToBytes(31497863415); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(int8(-117)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(int16(3847)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(int32(-7842245)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(int64(22416315751)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(uint(31497863415)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(uint8(117)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(uint16(3847)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(uint32(7842245)); err != nil {
		t.Error(err)
		return
	}
	if _, err := IntToBytes(uint64(22416315751)); err != nil {
		t.Error(err)
		return
	}
}

func TestIntOrdering(t *testing.T) {
	neg, _ := IntToBytes(-1)
	null, _ := IntToBytes(0)
	pos, _ := IntToBytes(1)

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

	neg, _ = IntToBytes(int64(math.MinInt64))
	null, _ = IntToBytes(int64(0))
	pos, _ = IntToBytes(int64(math.MaxInt64))

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

	if _, err := IntToBytes(time.Now()); err == nil {
		t.Error(err)
		return
	}
}

func TestTimeConversion(t *testing.T) {
	if _, err := TimeToBytes(time.Now()); err != nil {
		t.Error(err)
		return
	}

	if _, err := TimeToBytes("is it time?"); err == nil {
		t.Error(err)
		return
	}
}
