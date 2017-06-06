package crud

import (
	"bytes"
	"strings"
)

type SQLBuild struct {
	count     int
	colums    []string
	container *bytes.Buffer
}

func NewSQLBuild(tableName string, colums ...string) *SQLBuild {
	build := new(SQLBuild)
	build.count = 0
	for _, v := range colums {
		build.colums = append(build.colums, v)
	}
	build.container = bytes.NewBufferString("INSERT INTO `" + tableName + "` (")
	for idx := range colums {
		colums[idx] = "`" + colums[idx] + "`"
	}
	build.container.WriteString(strings.Join(colums, ","))
	build.container.WriteString(") VALUES")
	return build
}

func (b *SQLBuild) AddValue(args ...string) {
	b.count++
	b.container.WriteByte('(')
	length := len(args) - 1
	for idx, arg := range args {
		b.container.WriteByte('\'')
		b.container.Write(([]byte(arg)))
		b.container.WriteByte('\'')
		if idx < length {
			b.container.WriteByte(',')
		}
	}
	b.container.WriteByte(')')
	b.container.WriteByte(',')
}

func (b *SQLBuild) AddMap(m map[string]string) {
	args := []string{}
	for _, colnm := range b.colums {
		args = append(args, m[colnm])
	}
	b.AddValue(args...)
}

func (b *SQLBuild) String() string {
	sql := b.container.String()
	return sql[:len(sql)-1]
}

func (b *SQLBuild) Count() int {
	return b.count
}
