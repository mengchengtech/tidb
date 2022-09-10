package mctech

import (
	"regexp"
	"strings"
)

type stringFilter struct {
	pattern string
	action  string
}

// Filter interface
type Filter interface {
	Match(text string) (bool, error)
}

// NewStringFilter function
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

// PrefixFilterPattern function
func PrefixFilterPattern(text string) string {
	return filterStartsWith + ":" + text
}

// SuffixFilterPattern function
func SuffixFilterPattern(text string) string {
	return filterEndsWith + ":" + text
}

const (
	filterStartsWith = "starts-with"
	filterEndsWith   = "ends-with"
	filterContains   = "contains"
	filterRegex      = "regex"
)

func (f *stringFilter) Match(text string) (bool, error) {
	var (
		matched bool
		err     error
	)
	switch f.action {
	case filterStartsWith:
		matched = strings.HasPrefix(text, f.pattern)
	case filterEndsWith:
		matched = strings.HasSuffix(text, f.pattern)
	case filterContains:
		matched = strings.Contains(text, f.pattern)
	case filterRegex:
		matched, err = regexp.MatchString(f.pattern, text)
	default:
		matched = f.pattern == text
	}
	return matched, err
}
