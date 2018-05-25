package index

import (
	"github.com/alexandreStein/GoTinyDB/vars"
	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"
)

// NewString returns Index interface ready to manage string types
func NewString(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWithStringComparator(vars.TreeOrder)
	i.indexType = utils.StringComparatorType

	return i
}

// NewInt returns Index interface ready to manage int types
func NewInt(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWithIntComparator(vars.TreeOrder)
	i.indexType = utils.IntComparatorType

	return i
}

// NewTime returns Index interface ready to manage int types
func NewTime(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.TimeComparator, utils.TimeComparatorType)
	i.indexType = utils.TimeComparatorType

	return i
}

// NewInt8 returns Index interface ready to manage int types
func NewInt8(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Int8Comparator, utils.Int8ComparatorType)
	i.indexType = utils.Int8ComparatorType

	return i
}

// NewInt16 returns Index interface ready to manage int types
func NewInt16(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Int16Comparator, utils.Int16ComparatorType)
	i.indexType = utils.Int16ComparatorType

	return i
}

// NewInt32 returns Index interface ready to manage int types
func NewInt32(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Int32Comparator, utils.Int32ComparatorType)
	i.indexType = utils.Int32ComparatorType

	return i
}

// NewInt64 returns Index interface ready to manage int types
func NewInt64(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Int64Comparator, utils.Int64ComparatorType)
	i.indexType = utils.Int64ComparatorType

	return i
}

// NewUint returns Index interface ready to manage int types
func NewUint(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.UIntComparator, utils.UIntComparatorType)
	i.indexType = utils.UIntComparatorType

	return i
}

// NewUint8 returns Index interface ready to manage int types
func NewUint8(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.UInt8Comparator, utils.UInt8ComparatorType)
	i.indexType = utils.UInt8ComparatorType

	return i
}

// NewUint16 returns Index interface ready to manage int types
func NewUint16(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.UInt16Comparator, utils.UInt16ComparatorType)
	i.indexType = utils.UInt16ComparatorType

	return i
}

// NewUint32 returns Index interface ready to manage int types
func NewUint32(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.UInt32Comparator, utils.UInt32ComparatorType)
	i.indexType = utils.UInt32ComparatorType

	return i
}

// NewUint64 returns Index interface ready to manage int types
func NewUint64(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.UInt64Comparator, utils.UInt64ComparatorType)
	i.indexType = utils.UInt64ComparatorType

	return i
}

// NewFloat32 returns Index interface ready to manage int types
func NewFloat32(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Float32Comparator, utils.Float32ComparatorType)
	i.indexType = utils.Float32ComparatorType

	return i
}

// NewFloat64 returns Index interface ready to manage int types
func NewFloat64(path string, selector []string) Index {
	i := newStructIndex(path, selector)
	i.tree = btree.NewWith(vars.TreeOrder, utils.Float64Comparator, utils.Float64ComparatorType)
	i.indexType = utils.Float64ComparatorType

	return i
}

func newStructIndex(path string, selector []string) *structIndex {
	return &structIndex{
		path:     path,
		selector: selector,
	}
}
