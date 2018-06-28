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

	if IndexType(-1).TypeName() != "" {
		t.Error("returned name is not correct")
		return
	}
}

func TestBuildSelectorHash(t *testing.T) {
	selectors := [][]string{
		{"userName"},
		{"auth", "ssh"},
		{"email"},
	}
	expectedResults := []uint64{
		469024096205709603,
		256693521140565194,
		17382524093592847791,
	}

	for i := range selectors {
		if ret := BuildSelectorHash(selectors[i]); ret != expectedResults[i] {
			t.Errorf("wrong result expected %d but had %d", expectedResults[i], ret)
		}
	}

}
