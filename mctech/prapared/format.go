package prapared

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
)

type valueFormatter interface {
	Format(name string, value string) (any, error)
}

var booleanValues = []string{"true", "false", "1", "0"}

type booleanValueFormatter struct {
}

func (f *booleanValueFormatter) Format(name string, value string) (any, error) {
	return f.format(name, value)
}

func (f *booleanValueFormatter) format(name string, value string) (bool, error) {
	var index = -1
	for i, item := range booleanValues {
		if item == strings.ToLower(value) {
			index = i
			break
		}
	}

	if index < 0 {
		err := fmt.Errorf("%s的值错误。可选值为'true', 'false', '1', '0'", name)
		return false, err
	}

	v := booleanValues[index]
	return v == "true" || v == "1", nil
}

type globalValueFormatter struct {
	boolFormatter valueFormatter
}

var tokenSplitterPattern = regexp.MustCompile(`,|\s+`)

func newGlobalValueFormatter() valueFormatter {
	return &globalValueFormatter{
		boolFormatter: &booleanValueFormatter{},
	}
}

func (f *globalValueFormatter) Format(name string, value string) (any, error) {
	return f.format(name, value)
}

func (f *globalValueFormatter) format(name string, value string) (*mctech.GlobalValueInfo, error) {
	gv := new(mctech.GlobalValueInfo)
	global, err := f.boolFormatter.Format(name, value)

	if err == nil {
		gv.Global = global.(bool)
	} else {
		if value == "" {
			return nil, err
		}

		tokens := tokenSplitterPattern.Split(value, -1)

		// 当作要包含或排除的模式处理
		gv.Global = true
		for _, token := range tokens {
			if token == "" {
				continue
			}

			if !strings.HasPrefix(token, "!") {
				return nil, err
			}

			gv.Excludes = append(gv.Excludes, token[1:])
		}
	}

	return gv, nil
}
