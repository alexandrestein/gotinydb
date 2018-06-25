package gotinydb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/btree"
)

func TestIDsLess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	small, big := buildSmallAndBig(ctx, t)

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

func TestIDsOccurrenceIterators(t *testing.T) {
	tree := btree.New(3)
	count := 0

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for i := 100; i < 1000; i++ {
		count++
		id := NewID(ctx, fmt.Sprintf("%d", i))
		id.Increment()
		tree.ReplaceOrInsert(id.treeItem())
	}

	iter, ret := occurrenceTreeIterator(1, 20, 0, nil)
	tree.Ascend(iter)
	if ret.Len() != 20 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = occurrenceTreeIterator(1, 10, 0, nil)
	tree.Descend(iter)
	if ret.Len() != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	small, big := buildSmallAndBig(ctx, t)

	iter, ret = occurrenceTreeIterator(1, 10, 0, nil)
	tree.AscendGreaterOrEqual(small, iter)
	if ret.Len() != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = occurrenceTreeIterator(1, 10, 0, nil)
	tree.AscendLessThan(small, iter)
	if ret.Len() != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	iter, ret = occurrenceTreeIterator(1, 10, 0, nil)
	tree.DescendRange(big, small, iter)
	if ret.Len() != 10 {
		t.Errorf("returned value are not long as expected")
		return
	}

	// Test incrementassion
	overChan := make(chan bool, 1000)
	funcAddIncremental := func(next btree.Item) (over bool) {
		nextAsID, ok := next.(*ID)
		if !ok {
			return false
		}
		nextAsID.Increment()
		overChan <- true
		return true
	}

	nbOccurrences := 3
	for i := 0; i < nbOccurrences; i++ {
		go tree.Ascend(funcAddIncremental)
	}
	for index := 0; index < count*nbOccurrences; index++ {
		<-overChan
	}

	iter, ret = occurrenceTreeIterator(4, 20, 0, nil)
	tree.Ascend(iter)
	if ret.Len() != 20 {
		t.Errorf("returned value are %d not long as expected %d", ret.Len(), 20)
		return
	}
}

func buildSmallAndBig(ctx context.Context, t *testing.T) (small, big *ID) {
	smallVar, bigVar := NewID(ctx, "2000"), NewID(ctx, "7000")
	return smallVar, bigVar
}
