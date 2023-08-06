package intest

import (
	"os"
)

var InTest = false

func init() {
	for _, arg := range os.Args {
		if len(arg) >= 6 && arg[:6] == "-test." {
			InTest = true
			break
		}
	}
}
