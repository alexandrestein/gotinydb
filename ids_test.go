package gotinydb

import (
	"fmt"
	"testing"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/google/btree"
)

func TestIDsLess(t *testing.T) {
	small, big := buildSmallAndBig()

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
	for i := -100; i < 200; i++ {
		idsAsBytes, _ := vars.IntToBytes(i)
		ids := NewIDs(idsAsBytes)
		tree.ReplaceOrInsert(ids.treeItem())
	}

	fmt.Println("len", tree.Len())

	idsAsBytes, _ := vars.IntToBytes(-2)
	ids := NewIDs(idsAsBytes)
	fmt.Println("ids", ids)

	// iter, ret := iterator(20)
	// tree.Ascend(iter)
	// if len(ret.IDs) != 20 {
	// 	t.Errorf("returned value are not long as expected")
	// 	return
	// }

	// iter, ret = iterator(10)
	// tree.Descend(iter)
	// if len(ret.IDs) != 10 {
	// 	t.Errorf("returned value are not long as expected")
	// 	return
	// }

	small, big := buildSmallAndBig()

	// iter, ret = iterator(10)
	// tree.AscendGreaterOrEqual(small, iter)
	// for i, ids := range ret.IDs {
	// 	fmt.Println("ids, AscendGreaterOrEqual: ", i, ids.bytes)
	// }
	// if len(ret.IDs) != 10 {
	// 	t.Errorf("returned value are not long as expected")
	// 	return
	// }

	iter, ret := iterator(10)
	fmt.Println("small", small)
	tree.AscendLessThan(small, iter)
	for i, ids := range ret.IDs {
		fmt.Println("ids, AscendLessThan: ", i, ids.bytes)
	}
	if len(ret.IDs) != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = iterator(10)
	tree.DescendRange(big, small, iter)
	for i, ids := range ret.IDs {
		fmt.Println("ids, DescendRange: ", i, ids.bytes)
	}
	if len(ret.IDs) != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}
}

func buildSmallAndBig() (small, big *IDs) {
	smallAsBytes, _ := vars.IntToBytes(1)
	bigAsBytes, _ := vars.IntToBytes(100)
	return NewIDs(smallAsBytes), NewIDs(bigAsBytes)
}
