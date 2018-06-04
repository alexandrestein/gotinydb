package vars

import (
	"testing"
)

func TestBuildID(t *testing.T) {
	if id := BuildIDAsString("testString"); id != "570ad690bccc19cb28d2af1e9ccac359" {
		t.Error("returned ID, is not correct", id)
	}
}
