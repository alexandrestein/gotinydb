package index

import (
	"testing"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
)

func TestStringIndexWithSelector(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{"add", "street", "name"})
	i.getTree().Clear()
}

func TestIntIndexWithSelector(t *testing.T) {
	i := NewIntIndex(internalTesting.Path, []string{"add", "street", "num"})
	i.getTree().Clear()
}
