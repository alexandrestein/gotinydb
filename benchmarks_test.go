package gotinydb

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/minio/highwayhash"
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
	b.Run("BenchmarkInsertWithOneIndex", benchmarkInsertWithOneIndex)
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

// buildID returns ID as base 64 representation into a string
func buildID(id string) string {
	return base64.RawURLEncoding.EncodeToString(buildIDInternal(id))
}

// buildIDInternal builds an ID as a slice of bytes from the given string
func buildIDInternal(id string) []byte {
	key := make([]byte, highwayhash.Size)
	hash := highwayhash.Sum128([]byte(id), key)
	return []byte(hash[:])
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
	fmt.Printf("Fill up the database with %d records up to 1KB each\n", n)

	modulo := foundPourcentDivider(n)

	pourcent := 0
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			putRandRecord(benchmarkCollection, buildID(fmt.Sprint(i)))
			wg.Done()
		}()

		if i != 0 {
			if (i*10)%modulo == 0 {
				wg.Wait()
				if pourcent > 0 {
					fmt.Printf("%d0%%\n", pourcent)
				}
				pourcent++
			}
		}
	}
	fmt.Printf("100%% done\n")
	return nil
}

func foundPourcentDivider(n int) int {
	ret := 1
	for {
		if n/ret <= 1 {
			return ret
		}
		ret = ret * 10
	}
}

func initbenchmark(ctx context.Context) error {
	if initBenchmarkDone {
		return nil
	}

	initBenchmarkDone = true

	testPath := "benchmarkPath"
	os.RemoveAll(testPath)

	benchmarkDB, _ = Open(ctx, NewDefaultOptions(testPath))
	benchmarkCollection, _ = benchmarkDB.Use("testCol")
	deleteCollection, _ = benchmarkDB.Use("test del")

	users := unmarshalDataset(dataset1)

	iForIds := 0
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
				if i >= iForIds {
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
		query = NewQuery().SetFilter(NewEqualFilter("a", "email"))
	} else {
		query = NewQuery().
			SetFilter(NewEqualAndBetweenFilter("a", "b", "email")).
			SetFilter(NewEqualFilter(10, "Age")).
			SetFilter(NewEqualAndGreaterFilter(10000, "Balance")).
			SetFilter(NewEqualAndLessFilter(-100000, "Balance"))
	}

	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := benchmarkCollection.Query(query)
				if err != nil {
					b.Fatal(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			_, err := benchmarkCollection.Query(query)
			if err != nil {
				return err
			}
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
	benchmarkCollection.SetIndex("age", UIntIndex, "Age")
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
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

	b.ResetTimer()
	err := query(b, false, true)
	if err != nil {
		b.Error(err)
		return
	}

	delSixIndex()
}

func benchmarkQueryParallel(b *testing.B) {
	setSixIndex()

	b.ResetTimer()
	err := query(b, true, true)
	if err != nil {
		b.Error(err)
		return
	}

	delSixIndex()
}

func benchmarkQueryComplex(b *testing.B) {
	setSixIndex()

	b.ResetTimer()
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

	b.ResetTimer()
	err := query(b, true, false)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
	delSixIndex()
}
