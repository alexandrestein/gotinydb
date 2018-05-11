package collection

import "gitea.interlab-net.com/alexandre/db/index"

type (
	Collection struct {
		Indexes map[string]index.Index
		Meta    *MetaData
		path    string
	}

	MetaData struct{}
)
