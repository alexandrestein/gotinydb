package vars

import (
	"testing"
)

func TestBuildID(t *testing.T) {
	if id := BuildBytesID("testString"); string(id) != "VwrWkLzMGcso0q8enMrDWQ" {
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
