package testing

import "os"

var (
	Path = os.TempDir() + "/dbTest"
)

func init() {
	os.RemoveAll(Path)
}
