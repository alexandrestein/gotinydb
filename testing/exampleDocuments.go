package testing

import (
	"bytes"
	"math/rand"
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
		Age                    int
		Creation               time.Time

		Int8  int8
		Int16 int16
		Int32 int32
		Int64 int64

		Uint   uint
		Uint8  uint8
		Uint16 uint16
		Uint32 uint32
		Uint64 uint64

		Float32 float32
		Float64 float64
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
		&UserTest{"ID_USER_1", "mister one", "pass 1", 15, time.Now().Truncate(time.Minute), -1, -1, -1, -1, 1, 1, 1, 1, 1, 0.1, 0.1},
		&UserTest{"ID_USER_2", "user two", "pass 2", 30, time.Now().Add(time.Hour * 3600).Truncate(time.Minute), -2, -2, -2, -2, 2, 2, 2, 2, 2, 0.2, 0.2},
		&UserTest{"ID_USER_3", "lady three", "pass 3", 9223372036854775807, time.Now().Add(time.Hour * 3600).Truncate(time.Minute), -3, -3, -3, -3, 3, 3, 3, 3, 3, 0.3, 0.3},
	}
}

func GetCompleteUsersExample() []TestValue {
	ret := []TestValue{}

	ret = append(ret, GetCompleteUsersExampleOneAndTow()...)
	ret = append(ret, GetCompleteUsersExampleStreetNamesOnly()...)

	return ret
}

func GetCompleteUsersExampleOneAndTow() []TestValue {
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
func GetCompleteUsersExampleStreetNamesOnly() []TestValue {
	return []TestValue{
		&CompleteUser{ID: "S_North_1", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_2", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_3", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_4", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_5", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_6", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_7", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_8", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_9", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_10", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_11", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_12", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_13", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_14", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_15", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_16", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_17", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_18", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_19", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_20", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_21", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_22", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_23", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_24", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_25", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_26", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_27", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_28", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_29", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_30", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_31", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_32", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_33", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_34", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_35", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_36", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_37", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_38", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_39", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_40", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_41", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_42", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_43", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_44", Add: &Add{Street: &Street{Name: "East street"}}},
		&CompleteUser{ID: "S_George_45", Add: &Add{Street: &Street{Name: "George street"}}},
		&CompleteUser{ID: "S_North_46", Add: &Add{Street: &Street{Name: "North street"}}},
		&CompleteUser{ID: "S_South_47", Add: &Add{Street: &Street{Name: "South street"}}},
		&CompleteUser{ID: "S_West_48", Add: &Add{Street: &Street{Name: "West street"}}},
		&CompleteUser{ID: "S_East_49", Add: &Add{Street: &Street{Name: "East street"}}},

		&CompleteUser{ID: "DUP_1", Add: &Add{Street: &Street{Name: "North street Dup"}}},
		&CompleteUser{ID: "DUP_1", Add: &Add{Street: &Street{Name: "North street Dup1"}}},
		&CompleteUser{ID: "DUP_1", Add: &Add{Street: &Street{Name: "North street Dup2"}}},
		&CompleteUser{ID: "DUP_1", Add: &Add{Street: &Street{Name: "North street Dup3"}}},
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
