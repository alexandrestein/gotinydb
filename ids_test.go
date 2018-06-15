package gotinydb

import (
	"fmt"
	"testing"

	"github.com/google/btree"
)

func TestIDsLess(t *testing.T) {
	small, big := buildSmallAndBig(t)

	if !small.Less(big) {
		t.Error("small is declared as not smaller than big")
		return
	}
	if small.Less(small) {
		t.Error("small is declared as not smaller than small but equal values must returns false")
		return
	}
	if big.Less(small) {
		t.Error("big is declared as smaller than small")
		return
	}
}

func TestIDsIterators(t *testing.T) {
	tree := btree.New(3)
	for i := 1000; i < 10000; i++ {
		id := NewID(fmt.Sprintf("%d", i))
		tree.ReplaceOrInsert(id.treeItem())
	}

	iter, ret := iterator(20)
	tree.Ascend(iter)
	if len(ret.IDs) != 20 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = iterator(10)
	tree.Descend(iter)
	if len(ret.IDs) != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	small, big := buildSmallAndBig(t)

	iter, ret = iterator(10)
	tree.AscendGreaterOrEqual(small, iter)
	if len(ret.IDs) != 10 {
		fmt.Println(ret.IDs)
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = iterator(10)
	tree.AscendLessThan(small, iter)
	if len(ret.IDs) != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = iterator(10)
	tree.DescendRange(big, small, iter)
	if len(ret.IDs) != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}
}

func buildSmallAndBig(t *testing.T) (small, big *ID) {
	smallVar, bigVar := NewID("2000"), NewID("7000")
	return smallVar, bigVar
}
