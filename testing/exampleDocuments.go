package testing

import (
	"bytes"
	"crypto/rand"
	"reflect"
	"time"
)

type (
	TestValue interface {
		GetID() string
		GetContent() interface{}
		New() interface{}
		IsEqual(with interface{}) bool
	}

	UserTest struct {
		ID, UserName, Password string
		Creation               time.Time
	}

	RawTest struct {
		ID      string
		Content []byte
	}

	CompleteUser struct {
		ID    string
		Name  string
		Phone string
		Add   *Add
	}
	Add struct {
		Street *Street
		City   *City
	}
	Street struct {
		Name string
		Num  int
	}
	City struct {
		Zip  int
		Name string
	}
)

func (self *UserTest) GetID() string {
	return self.ID
}
func (self *UserTest) GetContent() interface{} {
	return self
}
func (self *UserTest) New() interface{} {
	return new(UserTest)
}
func (self *UserTest) IsEqual(with interface{}) bool {
	return reflect.DeepEqual(self.GetContent(), with)
}

func (self *CompleteUser) GetID() string {
	return self.ID
}
func (self *CompleteUser) GetContent() interface{} {
	return self
}
func (self *CompleteUser) New() interface{} {
	return new(CompleteUser)
}
func (self *CompleteUser) IsEqual(with interface{}) bool {
	return reflect.DeepEqual(self.GetContent(), with)
}

func (self *RawTest) GetID() string {
	return self.ID
}
func (self *RawTest) GetContent() interface{} {
	return self.Content
}
func (self *RawTest) New() interface{} {
	return bytes.NewBuffer(nil)
}
func (self *RawTest) IsEqual(with interface{}) bool {
	buff, ok := with.(*bytes.Buffer)
	if !ok {
		return false
	}
	return reflect.DeepEqual(self.GetContent(), buff.Bytes())
}

func GetUsersExample() []TestValue {
	// Time is truncate because the JSON format do not support nanosecondes
	return []TestValue{
		&UserTest{"ID_USER_1", "mister 1", "pass 1", time.Now().Truncate(time.Millisecond)},
		&UserTest{"ID_USER_2", "mister 2", "pass 2", time.Now().Add(time.Hour * 3600).Truncate(time.Millisecond)},
	}
}

func GetCompleteUsersExample() []TestValue {
	return []TestValue{
		&CompleteUser{ID: "ID_1",
			Name:  "Mister 1",
			Phone: "732-757-2923",
			Add: &Add{
				Street: &Street{
					Name: "Main street",
					Num:  135,
				},
				City: &City{
					Zip:  79321,
					Name: "Nice City",
				},
			}},
		&CompleteUser{ID: "ID_2",
			Name:  "Miss 2",
			Phone: "732-757-2923",
			Add: &Add{
				Street: &Street{
					Num: 364,
				},
				City: &City{
					Zip:  315154,
					Name: "Nice City",
				},
			}},
	}
}

func GetRawExample() []TestValue {
	return []TestValue{
		&RawTest{"ID_RAW_1", genRand(1024)},
		&RawTest{"ID_RAW_2", genRand(512)},
	}
}

func genRand(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}
