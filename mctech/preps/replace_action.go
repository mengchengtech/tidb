package preps

import (
	"fmt"
	"strings"
)

type action interface {
	Resolve(input string, args string, params map[string]any) (string, error)
}

type replaceAction struct {
}

func (a *replaceAction) Resolve(input string, args string, params map[string]any) (string, error) {
	var (
		name  string
		value string
	)

	if strings.Contains(args, "=") {
		tokens := strings.SplitN(args, "=", 2)
		name = tokens[0]
		value = tokens[1]

		if value[0] == '\'' && value[len(value)-1] == '\'' {
			value = value[1 : len(value)-1]
		}
	} else {
		name = args
		var ok bool
		v, ok := params[name]
		if ok {
			value, _ = v.(string)
		}

		if value == "" {
			err := fmt.Errorf("执行[replace]时未找到名称为'%s'的参数的值", name)
			return "", err
		}
	}

	return strings.ReplaceAll(input, "{{"+name+"}}", value), nil
}
