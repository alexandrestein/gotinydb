package gotinydb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

var (
	getID      chan string
	getSentID  chan string
	getContent chan interface{}

	initBenchmarkDone = false
)

func initbenchmark() {
	if initBenchmarkDone {
		return
	}
	initBenchmarkDone = true

	users := unmarshalDataSet(dataSet1)

	iForIds := 0
	getID = make(chan string, 100)
	go func() {
		for {
			getID <- buildID(fmt.Sprint(iForIds))
			iForIds++
		}
	}()
	getSentID = make(chan string, 100)
	go func() {
		i := 0
		for {
			if i < iForIds {
				getID <- buildID(fmt.Sprint(i))
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

func insertStructs(ctx context.Context, b *testing.B, parallel bool) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	initbenchmark()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := c.Put(<-getID, <-getContent)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			err := c.Put(<-getID, <-getContent)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func readStructs(b *testing.B, c *Collection, parallel bool) {
	initbenchmark()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := c.Get(<-getSentID, nil)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			_, err := c.Get(<-getSentID, nil)
			if err != nil {
				b.Error(err)
				return
			}
		}
	}
}

func BenchmarkInsertStructs(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertStructs(ctx, b, false)
}

func BenchmarkInsertStructsParallel(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertStructs(ctx,b, true)
}

func BenchmarkInsertStructsWithOneIndex(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")

	insertStructs(ctx,b, false)
}

func BenchmarkInsertStructsParallelWithOneIndex(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")

	insertStructs(b, c, true)
}

func BenchmarkInsertStructsWithSixIndexes(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")
	c.SetIndex("balance", IntIndex, "Balance")
	c.SetIndex("city", StringIndex, "Address", "City")
	c.SetIndex("zip", IntIndex, "Address", "ZipCode")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last login", TimeIndex, "LastLogin")

	insertStructs(b, c, false)
}

func BenchmarkInsertStructsParallelWithSixIndexes(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")
	c.SetIndex("balance", IntIndex, "Balance")
	c.SetIndex("city", StringIndex, "Address", "City")
	c.SetIndex("zip", IntIndex, "Address", "ZipCode")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last login", TimeIndex, "LastLogin")

	insertStructs(b, c, true)
}

func BenchmarkReadStructs(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	readStructs(b, c, false)
}

func BenchmarkReadStructsParallel(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	readStructs(b, c, true)
}

func BenchmarkReadStructsWithOneIndex(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")

	readStructs(b, c, false)
}

func BenchmarkReadStructsParallelWithOneIndex(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")

	readStructs(b, c, true)
}

func BenchmarkReadStructsWithSixIndexes(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")
	c.SetIndex("balance", IntIndex, "Balance")
	c.SetIndex("city", StringIndex, "Address", "City")
	c.SetIndex("zip", IntIndex, "Address", "ZipCode")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last login", TimeIndex, "LastLogin")

	readStructs(b, c, false)
}

func BenchmarkReadStructsParallelWithSixIndexes(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)

	db, _ := Open(ctx, testPath)
	c, _ := db.Use("testCol")

	c.SetIndex("email", StringIndex, "Email")
	c.SetIndex("balance", IntIndex, "Balance")
	c.SetIndex("city", StringIndex, "Address", "City")
	c.SetIndex("zip", IntIndex, "Address", "ZipCode")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last login", TimeIndex, "LastLogin")

	readStructs(b, c, true)
}
