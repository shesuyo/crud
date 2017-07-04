package crud

import (
	"fmt"
	"strings"
)

type joincon struct {
	tablename string
	condition string
}

type wherecon struct {
	query string
	args  []interface{}
}

type search struct {
	db              *CRUD
	fields          []string
	tableName       string
	joinConditions  []joincon
	whereConditions []wherecon
	group           string
	with            string
	having          string
	limit           interface{}
	offset          interface{}

	query string
	args  []interface{}

	raw bool
}

func (s *search) clone() *search {
	clone := *s
	return &clone
}

func (s *search) Fields(args ...string) *search {
	for i := 0; i < len(args); i++ {
		args[i] = "`" + args[i] + "`"
	}
	s.fields = append(s.fields, args...)
	return s
}

func (s *search) Where(query string, values ...interface{}) *search {
	s.whereConditions = append(s.whereConditions, wherecon{query: query, args: values})
	return s
}

func (s *search) Joins(tablename string, condition ...string) *search {
	if len(condition) == 1 {
		s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: condition[0]})
	}
	return s
}

func (s *search) TableName(name string) *search {
	s.tableName = name
	return s
}

func (s *search) Limit(limit interface{}) *search {
	s.limit = limit
	return s
}

func (s *search) Offset(offset interface{}) *search {
	s.offset = offset
	return s
}

func (s *search) Group(query string) *search {
	s.group = query
	return s
}

func (s *search) Parse() (string, []interface{}) {
	if s.raw == true {
		return s.query, s.args
	}
	var (
		fields       string
		joins        string
		paddingwhere string
		wheres       []string
		limit        string
		offset       string
	)
	s.query = ""
	s.args = []interface{}{}
	if len(s.fields) == 0 {
		fields = "*"
	} else {
		fields = strings.Join(s.fields, ",")
	}
	for _, joincon := range s.joinConditions {
		joins += fmt.Sprintf(" LEFT JOIN %s ON %s", joincon.tablename, joincon.condition)
	}
	for _, wherecon := range s.whereConditions {
		paddingwhere = " WHERE "
		wheres = append(wheres, wherecon.query)
		s.args = append(s.args, wherecon.args...)
	}
	if s.limit != nil {
		limit = " LIMIT ?"
		s.args = append(s.args, s.limit)
	}
	if s.offset != nil {
		offset = " OFFSET ?"
		s.args = append(s.args, s.offset)
	}
	s.query = fmt.Sprintf("SELECT %s FROM %s%s%s%s%s%s", fields, s.tableName, joins, paddingwhere, strings.Join(wheres, " AND "), limit, offset)
	s.raw = true
	return s.query, s.args
}
