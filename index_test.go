package db

import (
	"fmt"

	"gitea.interlab-net.com/alexandre/db/collection"
	"gitea.interlab-net.com/alexandre/db/index"
)

const (
	userNameIndexName = "userName"
	ageIndexName      = "age"
	creationIndexName = "creation"
)

func setIndexes(col *collection.Collection) error {
	if err := col.SetIndex(userNameIndexName, index.StringType, []string{"UserName"}); err != nil {
		return err
	}
	// Test duplicated index
	if err := col.SetIndex(userNameIndexName, index.StringType, []string{"UserName"}); err == nil {
		return fmt.Errorf("there is no error but the index allready exist")
	}
	if err := col.SetIndex(ageIndexName, index.IntType, []string{"Age"}); err != nil {
		return err
	}
	if err := col.SetIndex(creationIndexName, index.TimeType, []string{"Creation"}); err != nil {
		return err
	}
	return nil
}
