package testing

import (
	"crypto/rand"
	"time"
)

type (
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

func GetUsersExample() []*UserTest {
	// Time is truncate because the JSON format do not support nanosecondes
	return []*UserTest{
		&UserTest{"ID_USER_1", "mister 1", "pass 1", time.Now().Truncate(time.Millisecond)},
		&UserTest{"ID_USER_2", "mister 2", "pass 2", time.Now().Add(time.Hour * 3600).Truncate(time.Millisecond)},
	}
}

func GetCompleteUsersExample() []*CompleteUser {
	return []*CompleteUser{
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

func GetRawExample() []*RawTest {
	return []*RawTest{
		&RawTest{"ID_RAW_1", genRand(1024)},
		&RawTest{"ID_RAW_2", genRand(1024 * 1024 * 30)},
	}
}

func genRand(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}
