package vars

import (
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

	if _, err := IntToBytes(time.Now()); err == nil {
		t.Error(err)
		return
	}
}

func TestFloatConversion(t *testing.T) {
	if _, err := FloatToBytes(float32(349.154)); err != nil {
		t.Error(err)
		return
	}
	if _, err := FloatToBytes(float64(-487.934712)); err != nil {
		t.Error(err)
		return
	}

	if _, err := FloatToBytes(time.Now()); err == nil {
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
