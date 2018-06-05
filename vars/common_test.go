package vars

import (
	"testing"
)

func TestBuildID(t *testing.T) {
	if id := BuildIDAsString("testString"); id != "570ad690bccc19cb28d2af1e9ccac359" {
		t.Error("returned ID, is not correct", id)
	}
}

func TestTypeName(t *testing.T) {
	if StringIndex.TypeName() != "StringIndex" {
		t.Error("returned name is not correct")
		return
	}
	if IntIndex.TypeName() != "IntIndex" {
		t.Error("returned name is not correct")
		return
	}
	if FloatIndex.TypeName() != "FloatIndex" {
		t.Error("returned name is not correct")
		return
	}
	if TimeIndex.TypeName() != "TimeIndex" {
		t.Error("returned name is not correct")
		return
	}
	if BytesIndex.TypeName() != "BytesIndex" {
		t.Error("returned name is not correct")
		return
	}

	if IndexType(-1).TypeName() != "" {
		t.Error("returned name is not correct")
		return
	}
}
