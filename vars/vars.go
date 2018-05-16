// Package vars provides some of the global elements of the database.
package vars

import (
	"os"
)

// The values
const (
	BlockSize      = 1024 * 1024 * 10 // 10MB
	FilePermission = 0740             // u -> rwx | g -> r-- | o -> ---
	TreeOrder      = 10

	IndexesDirName = "indexes"
	RecordsDirName = "records"
	ObjectsDirName = "json"
	BinsDirName    = "bin"
	LockFileName   = "lock"

	OpenDBFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL

	GetFlags = os.O_RDONLY
	PutFlags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
)
