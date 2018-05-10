package db

import (
	"os"
)

var (
	path = os.TempDir() + "/dbTest"
)
