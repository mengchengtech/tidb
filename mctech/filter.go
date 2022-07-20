package mctech

import (
	"regexp"
	"strings"
)

type stringFilter struct {
	pattern string
	action  string
}

type Filter interface {
	Match(text string) (bool, error)
}

func NewStringFilter(pattern string) Filter {
	index := strings.Index(pattern, ":")
	filter := &stringFilter{}
	if index > 0 {
		filter.action = pattern[0:index]
		filter.pattern = pattern[index+1:]
	} else {
		filter.action = ""
		filter.pattern = pattern
	}

	if filter.action == "regex" {
		filter.pattern = "(?i)" + filter.pattern
	}
	return filter
}

func PrefixFilterPattern(text string) string {
	return FilterStartsWith + ":" + text
}

func SuffixFilterPattern(text string) string {
	return FilterEndsWith + ":" + text
}

const (
	FilterStartsWith = "starts-with"
	FilterEndsWith   = "ends-with"
	FilterContains   = "contains"
	FilterRegex      = "regex"
)

func (f *stringFilter) Match(text string) (bool, error) {
	var (
		matched bool
		err     error
	)
	switch f.action {
	case FilterStartsWith:
		matched = strings.HasPrefix(text, f.pattern)
	case FilterEndsWith:
		matched = strings.HasSuffix(text, f.pattern)
	case FilterContains:
		matched = strings.Contains(text, f.pattern)
	case FilterRegex:
		matched, err = regexp.MatchString(f.pattern, text)
	default:
		matched = f.pattern == text
	}
	return matched, err
}
