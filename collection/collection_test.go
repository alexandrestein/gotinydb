package collection

import (
	"reflect"
	"testing"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
)

func TestCollection(t *testing.T) {
	col, newColErr := NewCollection(internalTesting.Path)
	if newColErr != nil {
		t.Error(newColErr)
		return
	}

	users := internalTesting.GetUsersExample()

	for _, user := range users {
		putErr := col.Put(user.ID, user)
		if putErr != nil {
			t.Error(putErr)
			return
		}
		getUser := new(internalTesting.UserTest)
		getErr := col.Get(user.ID, getUser)
		if getErr != nil {
			t.Error(getErr)
			return
		}

		if !reflect.DeepEqual(user, getUser) {
			t.Errorf("returned object is not equal: %v\n%v", user, getUser)
			return
		}
	}

	// Update the values
	for i, user := range users {
		y := len(users) - 1 - i
		putErr := col.Put(user.ID, users[y])
		if putErr != nil {
			t.Error(putErr)
			return
		}

		getUser := new(internalTesting.UserTest)
		getErr := col.Get(user.ID, getUser)
		if getErr != nil {
			t.Error(getErr)
			return
		}

		if !reflect.DeepEqual(users[y], getUser) {
			t.Errorf("returned object is not equal: %v\n%v", users[y], getUser)
			return
		}
	}
}
