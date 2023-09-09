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
func NewStringFilter(pattern string) (Filter, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "*" {
		return nil, false
	}

	count := strings.Count(pattern, "*")
	var filter Filter
	if count == 0 {
		filter = &stringFilter{action: filterContains, pattern: pattern}
	} else if count == 1 {
		if strings.HasPrefix(pattern, "*") {
			filter = &stringFilter{action: filterEndsWith, pattern: pattern[1:]}
		} else if strings.HasSuffix(pattern, "*") {
			filter = &stringFilter{action: filterStartsWith, pattern: pattern[:len(pattern)-1]}
		}
	}

	if filter == nil {
		tokens := strings.Split(pattern, "*")
		var newTokens []string
		for _, tk := range tokens {
			newTokens = append(newTokens, regexp.QuoteMeta(tk))
		}
		pattern = strings.Join(newTokens, ".*")
		filter = &stringFilter{action: filterRegex, pattern: "(?i)" + pattern}
	}
	return filter, true
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
