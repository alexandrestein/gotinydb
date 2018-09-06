package gotinydb

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"runtime/debug"
	"testing"
	"time"
)

var (
	benchmarkDB                           *DB
	benchmarkCollection, deleteCollection *Collection

	getID         chan string
	getExistingID chan string
	getContent    chan interface{}

	initBenchmarkDone = false
)

func Benchmark(b *testing.B) {
	if testing.Short() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := initbenchmark(ctx)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	b.Run("BenchmarkInsert", benchmarkInsert)
	b.Run("BenchmarkInsertParallel", benchmarkInsertParallel)
	b.Run("BenchmarkInsertParallelWithOneIndex", benchmarkInsertParallelWithOneIndex)
	b.Run("BenchmarkInsertWithSixIndexes", benchmarkInsertWithSixIndexes)
	b.Run("BenchmarkInsertParallelWithSixIndexes", benchmarkInsertParallelWithSixIndexes)
	b.Run("BenchmarkRead", benchmarkRead)
	b.Run("BenchmarkReadParallel", benchmarkReadParallel)
	b.Run("BenchmarkReadWithOneIndex", benchmarkReadWithOneIndex)
	b.Run("BenchmarkReadParallelWithOneIndex", benchmarkReadParallelWithOneIndex)
	b.Run("BenchmarkReadWithSixIndexes", benchmarkReadWithSixIndexes)
	b.Run("BenchmarkReadParallelWithSixIndexes", benchmarkReadParallelWithSixIndexes)
	b.Run("BenchmarkDelete", benchmarkDelete)
	b.Run("BenchmarkDeleteParallel", benchmarkDeleteParallel)
	b.Run("BenchmarkDeleteWithOneIndex", benchmarkDeleteWithOneIndex)
	b.Run("BenchmarkDeleteParallelWithOneIndex", benchmarkDeleteParallelWithOneIndex)
	b.Run("BenchmarkDeleteWithSixIndexes", benchmarkDeleteWithSixIndexes)
	b.Run("BenchmarkDeleteParallelWithSixIndexes", benchmarkDeleteParallelWithSixIndexes)
	b.Run("benchmarkQuery", benchmarkQuery)
	b.Run("benchmarkQueryParallel", benchmarkQueryParallel)
	b.Run("benchmarkQueryComplex", benchmarkQueryComplex)
	b.Run("benchmarkQueryParallelComplex", benchmarkQueryParallelComplex)

	if err := benchmarkDB.Close(); err != nil {
		b.Error("closing: ", err)
	}

	cancel()

	time.Sleep(time.Second)
}

func putRandRecord(c *Collection, id string) error {
	// up to 1KB
	contentSize, err := rand.Int(rand.Reader, big.NewInt(1000*1))
	if err != nil {
		return err
	}

	content := make([]byte, contentSize.Int64())
	rand.Read(content)

	err = c.Put(id, content)
	if err != nil {
		return err
	}
	return nil
}

func fillUpDBForBenchmarks(n int) error {
	fmt.Println("Fill up the database with 1'000 records up to 1KB")
	pourcent := 0
	for i := 0; i < n; i++ {
		err := putRandRecord(benchmarkCollection, buildID(fmt.Sprint(i)))
		if err != nil {
			return err
		}

		if i%1000 == 0 {
			if i != 0 {
				fmt.Printf("%d0%%\n", pourcent)
			}
			pourcent++
		}
	}
	fmt.Printf("100%% done\n")
	return nil
}

func initbenchmark(ctx context.Context) error {
	if initBenchmarkDone {
		return nil
	}

	nbInitInsertion := 1000

	initBenchmarkDone = true

	testPath := "benchmarkPath"

	benchmarkDB, _ = Open(ctx, NewDefaultOptions(testPath))
	benchmarkCollection, _ = benchmarkDB.Use("testCol")
	deleteCollection, _ = benchmarkDB.Use("test del")

	if err := fillUpDBForBenchmarks(nbInitInsertion); err != nil {
		return err
	}
	users := unmarshalDataset(dataset1)

	iForIds := nbInitInsertion
	getID = make(chan string, 100)
	go func() {
		for {
			select {
			case getID <- buildID(fmt.Sprint(iForIds)):
				iForIds++
			case <-ctx.Done():
				os.RemoveAll(testPath)
				return
			}
		}
	}()
	getExistingID = make(chan string, 100)
	go func() {
		i := 0
		for {
			select {
			case getExistingID <- buildID(fmt.Sprint(i)):
				if i >= nbInitInsertion {
					i = 0
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	getContent = make(chan interface{}, 100)
	go func() {
		for {
			select {
			case getContent <- users[iForIds%299]:
			case <-ctx.Done():
				os.RemoveAll(testPath)
				return
			}
		}
	}()
	return nil
}

func insert(b *testing.B, parallel bool) error {
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := benchmarkCollection.Put(<-getID, <-getContent)
				if err != nil {
					b.Fatal(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			err := benchmarkCollection.Put(<-getID, <-getContent)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func read(b *testing.B, parallel bool) error {
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := benchmarkCollection.Get(<-getExistingID, nil)
				if err != nil {
					b.Fatal(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			_, err := benchmarkCollection.Get(<-getExistingID, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func delete(b *testing.B, parallel bool) error {
	b.StopTimer()
	nbOfSamples := 1000

	for i := 0; i < nbOfSamples; i++ {
		err := putRandRecord(deleteCollection, buildID(fmt.Sprintf("test-%d", i)))
		if err != nil {
			debug.PrintStack()
			return err
		}
	}

	getExistingIDToDelete := make(chan string, 100)
	go func() {
		i := 0
		for {
			if i < nbOfSamples {
				getExistingIDToDelete <- buildID(fmt.Sprintf("test-%d", i))
				i++
			} else {
				close(getExistingIDToDelete)
				return
			}
		}
	}()

	b.StartTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id, ok := <-getExistingIDToDelete
				if !ok {
					return
				}

				err := deleteCollection.Delete(id)
				if err != nil {
					b.Fatal(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			id, ok := <-getExistingIDToDelete
			if !ok {
				return nil
			}

			err := deleteCollection.Delete(id)
			if err != nil {
				debug.PrintStack()
				return err
			}
		}
	}

	return nil
}

func query(b *testing.B, parallel bool, simple bool) error {
	b.ResetTimer()

	var query *Query

	if simple {
		query = NewQuery().SetFilter(NewFilter(Greater).SetSelector("email").CompareTo("a"))
	} else {
		query = NewQuery().
			SetFilter(NewFilter(Between).SetSelector("email").CompareTo("a").CompareTo("b")).
			SetFilter(NewFilter(Equal).SetSelector("Age").CompareTo(10)).
			SetFilter(NewFilter(Greater).SetSelector("Balance").CompareTo(10000)).
			SetFilter(NewFilter(Less).SetSelector("Balance").CompareTo(-100000))
	}

	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				responseQuery, err := benchmarkCollection.Query(query)
				if err != nil {
					b.Fatal(err)
					return
				}

				fmt.Println("responseQuery", responseQuery.Len())
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			responseQuery, err := benchmarkCollection.Query(query)
			if err != nil {
				return err
			}

			fmt.Println("responseQuery", responseQuery.Len())
		}
	}

	return nil
}

func setOneIndex() {
	benchmarkCollection.SetIndex("email", StringIndex, "email")
}
func delOneIndex() {
	benchmarkCollection.DeleteIndex("email")
}
func setSixIndex() {
	benchmarkCollection.SetIndex("email", StringIndex, "email")
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

func benchmarkInsert(b *testing.B) {
	err := insert(b, false)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkInsertParallel(b *testing.B) {
	err := insert(b, true)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkInsertWithOneIndex(b *testing.B) {
	setOneIndex()

	err := insert(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkInsertParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	err := insert(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkInsertWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := insert(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkInsertParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := insert(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkRead(b *testing.B) {
	err := read(b, false)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkReadParallel(b *testing.B) {
	err := read(b, true)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkReadWithOneIndex(b *testing.B) {
	setOneIndex()

	err := read(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkReadParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	err := read(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkReadWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := read(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkReadParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := read(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkDelete(b *testing.B) {
	err := delete(b, false)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkDeleteParallel(b *testing.B) {
	err := delete(b, true)
	if err != nil {
		b.Error(err)
		return
	}
}

func benchmarkDeleteWithOneIndex(b *testing.B) {
	setOneIndex()

	err := delete(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkDeleteParallelWithOneIndex(b *testing.B) {
	setOneIndex()

	err := delete(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delOneIndex()
}

func benchmarkDeleteWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := delete(b, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkDeleteParallelWithSixIndexes(b *testing.B) {
	setSixIndex()

	err := delete(b, true)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkQuery(b *testing.B) {
	setSixIndex()
	err := query(b, false, true)
	if err != nil {
		b.Error(err)
		return
	}

	delSixIndex()
}

func benchmarkQueryParallel(b *testing.B) {
	setSixIndex()
	err := query(b, true, true)
	if err != nil {
		b.Error(err)
		return
	}

	delSixIndex()
}

func benchmarkQueryComplex(b *testing.B) {
	setSixIndex()

	err := query(b, false, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}

func benchmarkQueryParallelComplex(b *testing.B) {
	setSixIndex()

	err := query(b, true, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}
