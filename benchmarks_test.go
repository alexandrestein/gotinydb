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

func fillUpDBForBenchmarks() {
	for i := 0; i < 10000; i++ {
		// up to 1KB
		contentSize, err := rand.Int(rand.Reader, big.NewInt(1000*1))
		if err != nil {
			log.Fatalln(err)
			return
		}

		content := make([]byte, contentSize.Int64())
		rand.Read(content)

		err = benchmarkCollection.Put(buildID(fmt.Sprint(i)), content)
		if err != nil {
			log.Fatalln(err)
			return
		}
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

	iForIds := 10000
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
			if i < 10000 {
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

func insertStructs(b *testing.B, parallel bool) {
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

func readStructs(b *testing.B, parallel bool) {
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

func BenchmarkInsertStructs(b *testing.B) {
	insertStructs(b, false)
}

func BenchmarkInsertStructsParallel(b *testing.B) {
	insertStructs(b, true)
}

func BenchmarkInsertStructsWithOneIndex(b *testing.B) {
	setOneIndex()

	insertStructs(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkInsertStructsParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	insertStructs(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkInsertStructsWithSixIndexes(b *testing.B) {
	setSixIndex()

	insertStructs(b, false)

	b.StopTimer()
	benchmarkCollection.DeleteIndex("email")
	benchmarkCollection.DeleteIndex("balance")
	benchmarkCollection.DeleteIndex("city")
	benchmarkCollection.DeleteIndex("zip")
	benchmarkCollection.DeleteIndex("age")
	benchmarkCollection.DeleteIndex("last login")
}

func BenchmarkInsertStructsParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	insertStructs(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadStructs(b *testing.B) {
	readStructs(b, false)
}

func BenchmarkReadStructsParallel(b *testing.B) {
	readStructs(b, true)
}

func BenchmarkReadStructsWithOneIndex(b *testing.B) {
	setOneIndex()

	readStructs(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadStructsParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	readStructs(b, true)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadStructsWithSixIndexes(b *testing.B) {
	setSixIndex()

	readStructs(b, false)

	b.StopTimer()
	delOneIndex()
}

func BenchmarkReadStructsParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	readStructs(b, true)

	b.StopTimer()
	delOneIndex()
}
