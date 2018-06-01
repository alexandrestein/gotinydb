package gotinydb

import (
	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"
)

// NewStringIndex returns Index interface ready to manage string types
func NewStringIndex(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWithStringComparator(TreeOrder)
	i.indexType = utils.StringComparatorType

	return i
}

// NewIntIndex returns Index interface ready to manage int types
func NewIntIndex(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWithIntComparator(TreeOrder)
	i.indexType = utils.IntComparatorType

	return i
}

// NewTimeIndex returns Index interface ready to manage int types
func NewTimeIndex(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.TimeComparator, utils.TimeComparatorType)
	i.indexType = utils.TimeComparatorType

	return i
}

// NewInt8Index returns Index interface ready to manage int types
func NewInt8Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Int8Comparator, utils.Int8ComparatorType)
	i.indexType = utils.Int8ComparatorType

	return i
}

// NewInt16Index returns Index interface ready to manage int types
func NewInt16Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Int16Comparator, utils.Int16ComparatorType)
	i.indexType = utils.Int16ComparatorType

	return i
}

// NewInt32Index returns Index interface ready to manage int types
func NewInt32Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Int32Comparator, utils.Int32ComparatorType)
	i.indexType = utils.Int32ComparatorType

	return i
}

// NewInt64Index returns Index interface ready to manage int types
func NewInt64Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Int64Comparator, utils.Int64ComparatorType)
	i.indexType = utils.Int64ComparatorType

	return i
}

// NewUintIndex returns Index interface ready to manage int types
func NewUintIndex(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.UIntComparator, utils.UIntComparatorType)
	i.indexType = utils.UIntComparatorType

	return i
}

// NewUint8Index returns Index interface ready to manage int types
func NewUint8Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.UInt8Comparator, utils.UInt8ComparatorType)
	i.indexType = utils.UInt8ComparatorType

	return i
}

// NewUint16Index returns Index interface ready to manage int types
func NewUint16Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.UInt16Comparator, utils.UInt16ComparatorType)
	i.indexType = utils.UInt16ComparatorType

	return i
}

// NewUint32Index returns Index interface ready to manage int types
func NewUint32Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.UInt32Comparator, utils.UInt32ComparatorType)
	i.indexType = utils.UInt32ComparatorType

	return i
}

// NewUint64Index returns Index interface ready to manage int types
func NewUint64Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.UInt64Comparator, utils.UInt64ComparatorType)
	i.indexType = utils.UInt64ComparatorType

	return i
}

// NewFloat32Index returns Index interface ready to manage int types
func NewFloat32Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Float32Comparator, utils.Float32ComparatorType)
	i.indexType = utils.Float32ComparatorType

	return i
}

// NewFloat64Index returns Index interface ready to manage int types
func NewFloat64Index(name string, selector []string) Index {
	i := newStructIndex(name, selector)
	i.tree = btree.NewWith(TreeOrder, utils.Float64Comparator, utils.Float64ComparatorType)
	i.indexType = utils.Float64ComparatorType

	return i
}

func newStructIndex(name string, selector []string) *structIndex {
	return &structIndex{
		name:     name,
		selector: selector,
	}
}
