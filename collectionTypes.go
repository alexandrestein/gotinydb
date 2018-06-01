package GoTinyDB

import (
	bolt "github.com/coreos/bbolt"
)

type (
	// Collection define the main element of the database. This is where data are
	// stored. The design is similar to other NO-SQL database.
	Collection struct {
		Name    string
		Indexes map[string]Index

		// path    string
		boltDB *bolt.DB
	}
)
