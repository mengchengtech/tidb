package intest

import (
	"os"
)

// InTest 全局变量. 声明当前是否运行在测试用例环境中
var InTest = false

func init() {
	for _, arg := range os.Args {
		if len(arg) >= 6 && arg[:6] == "-test." {
			InTest = true
			break
		}
	}
}
