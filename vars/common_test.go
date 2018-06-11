package vars

import (
	"fmt"
	"testing"
)

func TestBuildID(t *testing.T) {
	if id := BuildID("testString"); id != "VwrWkLzMGcso0q8enMrDWQ" {
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

func TestIDsParser(t *testing.T) {
	idsAsByte := `["id0","id1"]`
	ids, err := ParseIDsBytesToIDsAsStrings([]byte(idsAsByte))
	if err != nil {
		t.Error(err)
		return
	}

	if len(ids) != 2 {
		t.Errorf("returned ids are not equal to 2: %v", ids)
		return
	}

	for i, id := range ids {
		if fmt.Sprintf("id%d", i) != id {
			t.Errorf("parse not correct. Expected %s but had %s", fmt.Sprintf("id%d", i), id)
		}
	}
}

func TestIDsFormat(t *testing.T) {
	ids, err := FormatIDsStringsToIDsAsBytes([]string{"id0", "id1"})
	if err != nil {
		t.Error(err)
		return
	}

	if string(ids) != `["id0","id1"]` {
		t.Errorf("fomated ids is %s but expected %s", string(ids), `["id0", "id1"]`)
		return
	}
}
