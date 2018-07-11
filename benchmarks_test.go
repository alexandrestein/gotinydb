package gotinydb

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"testing"
)

var (
	benchmarkDB         *DB
	benchmarkCollection *Collection

	getID         chan string
	getExistingID chan string
	getContent    chan interface{}

	initBenchmarkDone = false
)

func putRandRecord(id string) {
	// up to 1KB
	contentSize, err := rand.Int(rand.Reader, big.NewInt(1000*10))
	if err != nil {
		log.Fatalln(err)
		return
	}

	content := make([]byte, contentSize.Int64())
	rand.Read(content)

	err = benchmarkCollection.Put(id, content)
	if err != nil {
		log.Fatalln(err)
		return
	}
}

func fillUpDBForBenchmarks() {
	for i := 0; i < 100000; i++ {
		putRandRecord(buildID(fmt.Sprint(i)))
	}
}

func initbenchmark() {
	if initBenchmarkDone {
		return
	}
	initBenchmarkDone = true

	ctx := context.Background()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	benchmarkDB, _ = Open(ctx, testPath)
	benchmarkCollection, _ = benchmarkDB.Use("testCol")

	fillUpDBForBenchmarks()

	users := unmarshalDataSet(dataSet1)

	iForIds := 100000
	getID = make(chan string, 100)
	go func() {
		for {
			getID <- buildID(fmt.Sprint(iForIds))
			iForIds++
		}
	}()
	getExistingID = make(chan string, 100)
	go func() {
		i := 0
		for {
			if i < 100000 {
				getExistingID <- buildID(fmt.Sprint(i))
			} else {
				i = 0
			}
		}
	}()

	getContent = make(chan interface{}, 100)
	go func() {
		for {
			getContent <- users[iForIds%299]
		}
	}()
}

func insert(b *testing.B, parallel bool) {
	initbenchmark()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := benchmarkCollection.Put(<-getID, <-getContent)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			err := benchmarkCollection.Put(<-getID, <-getContent)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func read(b *testing.B, parallel bool) {
	initbenchmark()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := benchmarkCollection.Get(<-getExistingID, nil)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			_, err := benchmarkCollection.Get(<-getExistingID, nil)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func delete(b *testing.B, parallel bool) {
	initbenchmark()

	for i := 0; i < 100000; i++ {
		putRandRecord(buildID(fmt.Sprintf("test-%d", i)))
	}

	getExistingIDToDelete := make(chan string, 100)
	go func() {
		i := 0
		for {
			if i < 100000 {
				getExistingIDToDelete <- buildID(fmt.Sprintf("test-%d", i))
			} else {
				i = 0
			}
		}
	}()

	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := benchmarkCollection.Delete(<-getExistingIDToDelete)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			err := benchmarkCollection.Delete(<-getExistingIDToDelete)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func query(b *testing.B, parallel bool) {
	initbenchmark()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := benchmarkCollection.Get(<-getExistingID, nil)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			_, err := benchmarkCollection.Get(<-getExistingID, nil)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func setOneIndex() {
	benchmarkCollection.SetIndex("email", StringIndex, "Email")
}
func delOneIndex() {
	benchmarkCollection.DeleteIndex("email")
}
func setSixIndex() {
	benchmarkCollection.SetIndex("email", StringIndex, "Email")
	benchmarkCollection.SetIndex("balance", IntIndex, "Balance")
	benchmarkCollection.SetIndex("city", StringIndex, "Address", "City")
	benchmarkCollection.SetIndex("zip", IntIndex, "Address", "ZipCode")
	benchmarkCollection.SetIndex("age", IntIndex, "Age")
	benchmarkCollection.SetIndex("last login", TimeIndex, "LastLogin")
}
func delSixIndex() {
	benchmarkCollection.DeleteIndex("email")
	benchmarkCollection.DeleteIndex("balance")
	benchmarkCollection.DeleteIndex("city")
	benchmarkCollection.DeleteIndex("zip")
	benchmarkCollection.DeleteIndex("age")
	benchmarkCollection.DeleteIndex("last login")
}

func BenchmarkInsert(b *testing.B) {
	insert(b, false)
}

func BenchmarkInsertParallel(b *testing.B) {
	insert(b, true)
}

func BenchmarkInsertWithOneIndex(b *testing.B) {
	setOneIndex()

	insert(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkInsertParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	insert(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkInsertWithSixIndexes(b *testing.B) {
	setSixIndex()

	insert(b, false)

	b.StopTimer()
	delSixIndex()
}

func BenchmarkInsertParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	insert(b, true)

	b.StopTimer()
	delSixIndex()
}

func BenchmarkRead(b *testing.B) {
	read(b, false)
}

func BenchmarkReadParallel(b *testing.B) {
	read(b, true)
}

func BenchmarkReadWithOneIndex(b *testing.B) {
	setOneIndex()

	read(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	read(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadWithSixIndexes(b *testing.B) {
	setSixIndex()

	read(b, false)

	b.StopTimer()
	delSixIndex()
}

func BenchmarkReadParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	read(b, true)

	b.StopTimer()
	delSixIndex()
}

func BenchmarkDelete(b *testing.B) {
	delete(b, false)
}

func BenchmarkDeleteParallel(b *testing.B) {
	delete(b, true)
}

func BenchmarkDeleteWithOneIndex(b *testing.B) {
	setOneIndex()

	delete(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkDeleteParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	delete(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkDeleteWithSixIndexes(b *testing.B) {
	setSixIndex()

	delete(b, false)

	b.StopTimer()
	delSixIndex()
}

func BenchmarkDeleteParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	delete(b, true)

	b.StopTimer()
	delSixIndex()
}
