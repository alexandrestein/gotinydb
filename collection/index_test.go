package collection

import (
	"fmt"
	"testing"

	"github.com/emirpasic/gods/utils"
)

const (
	UserNameIndexName = "userName"
	AgeIndexName      = "age"
	CreationIndexName = "creation"

	Int8IndexName    = "int8"
	Int16IndexName   = "int16"
	Int32IndexName   = "int32"
	Int64IndexName   = "int64"
	UintIndexName    = "uint"
	Uint8IndexName   = "uint8"
	Uint16IndexName  = "uint16"
	Uint32IndexName  = "uint32"
	Uint64IndexName  = "uint64"
	Float32IndexName = "float32"
	Float64IndexName = "float64"
)

// is a clone of funcs.SetIndexes
func SetIndexes(t *testing.T, col *Collection) error {
	if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err != nil {
		t.Error(err)
	}
	// Test duplicated index
	if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err == nil {
		err = fmt.Errorf("there is no error but the index allready exist")
		t.Error(err)
	}
	if err := col.SetIndex(AgeIndexName, utils.IntComparatorType, []string{"Age"}); err != nil {
		t.Error(err)
	}

	if err := col.SetIndex(Int8IndexName, utils.Int8ComparatorType, []string{"Int8"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Int16IndexName, utils.Int16ComparatorType, []string{"Int16"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Int32IndexName, utils.Int32ComparatorType, []string{"Int32"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Int64IndexName, utils.Int64ComparatorType, []string{"Int64"}); err != nil {
		t.Error(err)
	}

	if err := col.SetIndex(UintIndexName, utils.UIntComparatorType, []string{"Uint"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Uint8IndexName, utils.UInt8ComparatorType, []string{"Uint8"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Uint16IndexName, utils.UInt16ComparatorType, []string{"Uint16"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Uint32IndexName, utils.UInt32ComparatorType, []string{"Uint32"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Uint64IndexName, utils.UInt64ComparatorType, []string{"Uint64"}); err != nil {
		t.Error(err)
	}

	if err := col.SetIndex(Float32IndexName, utils.Float32ComparatorType, []string{"Float32"}); err != nil {
		t.Error(err)
	}
	if err := col.SetIndex(Float64IndexName, utils.Float64ComparatorType, []string{"Float64"}); err != nil {
		t.Error(err)
	}

	if err := col.SetIndex(CreationIndexName, utils.TimeComparatorType, []string{"Creation"}); err != nil {
		t.Error(err)
	}

	return nil
}
