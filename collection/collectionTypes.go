package collection

import "github.com/alexandreStein/GoTinyDB/index"

type (
	// Collection define the main element of the database. This is where data are
	// stored. The design is similar to other NO-SQL database.
	Collection struct {
		indexes map[string]index.Index
		path    string
	}
)
