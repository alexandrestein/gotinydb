package testing

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/alexandreStein/GoTinyDB/vars"
)

var (
	Path = ""
)

func init() {
	randNum, err := rand.Int(rand.Reader, big.NewInt(int64(^uint(0)>>1)))
	if err != nil {
		log.Fatal(err)
	}
	Path = os.TempDir() + "/dbTest-" + fmt.Sprintf("%d", randNum)
	os.MkdirAll(Path, vars.FilePermission)
}
