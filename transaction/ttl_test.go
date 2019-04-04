package transaction

import (
	"testing"
)

func TestTTLStruct(t *testing.T) {
	if testing.Short() {
		return
	}

	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	// t1 := time.Now().Add(time.Hour)
	// t2 := t1.Add(time.Second)
	// t0 := time.Now().Add(-time.Second)

	// ttl1 := &ttl{
	// 	CleanTime: t1,
	// }
	// ttl2 := &ttl{
	// 	CleanTime: t2,
	// }
	// ttl0 := &ttl{
	// 	CleanTime: t0,
	// }

	// fmt.Println(ttl1.TimeAsKey(), ttl2.TimeAsKey(), ttl0.TimeAsKey())
}
