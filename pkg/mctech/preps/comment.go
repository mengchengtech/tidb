package preps

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/mctech"
)

// customComments sql中特殊的注释信息
type customComments struct {
	service mctech.ServiceComment // 执行sql的服务名称
	pkg     mctech.PackageComment // 执行sql所属的依赖包名称（公共包中执行的sql）
}

func (c *customComments) Service() mctech.ServiceComment {
	return c.service
}

func (c *customComments) Package() mctech.PackageComment {
	return c.pkg
}

func (c *customComments) GetInfoForTest() map[string]any {
	result := map[string]any{}
	if c.service != nil {
		result["service"] = c.service.From()
	}
	if c.pkg != nil {
		result["pkg"] = c.pkg.Name()
	}
	return result
}

// serviceComment service comment
type serviceComment struct {
	from        string // {appName}[.{productLine}]
	appName     string // 执行sql的服务名称
	productLine string // 执行sql所属的服务的产品线
}

func (c *serviceComment) From() string {
	return c.from
}

func (c *serviceComment) AppName() string {
	return c.appName
}

func (c *serviceComment) ProductLine() string {
	return c.productLine
}

// packageComment package comment
type packageComment struct {
	name string
}

func (c *packageComment) Name() string {
	return c.name
}

// NewComments create new Comments instance
func NewComments(from, pkg string) mctech.Comments {
	comments := &customComments{}
	if len(pkg) > 0 {
		comments.pkg = &packageComment{name: pkg}
	}

	tokens := strings.SplitN(from, ".", 2)
	appName := tokens[0]
	var productLine string
	if len(tokens) > 1 {
		productLine = tokens[1]
	}
	if len(appName) > 0 || len(productLine) > 0 {
		comments.service = &serviceComment{from: from, appName: appName, productLine: productLine}
	}
	return comments
}

var customCommentPattern = regexp.MustCompile(`(?i)/\*\s*(from|package):\s*'([^']+)'`)

// GetCustomCommentFromSQL 尝试从sql中提取特殊注释
func GetCustomCommentFromSQL(sql string) mctech.Comments {
	matches := customCommentPattern.FindAllStringSubmatch(sql, -1)
	var (
		from string
		pkg  string
	)
	for _, match := range matches {
		name := match[1]
		value := match[2]
		switch name {
		case mctech.CommentFrom:
			from = value
		case mctech.CommentPackage:
			pkg = value
		}
	}

	return NewComments(from, pkg)
}
