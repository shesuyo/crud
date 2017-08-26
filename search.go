package crud

import (
	"fmt"
	"strconv"
	"strings"
)

type joincon struct {
	tablename string
	condition string
}

type joincons []joincon

func (jc joincons) haveTable(tbName string) bool {
	for _, v := range jc {
		if v.tablename == tbName {
			return true
		}
	}
	return false
}

type wherecon struct {
	query string
	args  []interface{}
}

type search struct {
	db              *CRUD
	fields          []string
	tableName       string
	joinConditions  joincons
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
	//err `xxx.xxx`
	//err `xxx.xx AS xxx`
	//err `DISTINCT cid`
	//	for i := 0; i < len(args); i++ {
	//		switch args[i] {
	//		case "id":
	//			args[i] = s.tableName + ".id"
	//			continue
	//		case "*":
	//			continue
	//		}
	//		if strings.Contains(args[i], ".") || strings.Contains(args[i], "AS") || strings.Contains(args[i], "as") || strings.Contains(args[i], "DISTINCT") {

	//		} else {
	//			args[i] = "`" + args[i] + "`"
	//		}

	//	}
	s.fields = append(s.fields, args...)
	return s
}

func (s *search) Where(query string, values ...interface{}) *search {
	id, err := strconv.Atoi(query)
	if err != nil {
		s.whereConditions = append(s.whereConditions, wherecon{query: query, args: values})
	} else {
		s.whereConditions = append(s.whereConditions, wherecon{query: s.tableName + ".id = ?", args: []interface{}{id}})
	}
	return s
}
func (s *search) Joins(tablename string, condition ...string) *search {
	if len(condition) == 1 {
		s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: condition[0]})
	} else {
		if s.db.tableColumns[tablename].HaveColumn(s.tableName + "id") {
			s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"id", s.tableName)})
		} else if s.db.tableColumns[tablename].HaveColumn(s.tableName + "_id") {
			s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"_id", s.tableName)})
		} else if s.db.tableColumns[s.tableName].HaveColumn(tablename + "id") {
			s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"id", tablename)})
		} else if s.db.tableColumns[s.tableName].HaveColumn(tablename + "_id") {
			s.joinConditions = append(s.joinConditions, joincon{tablename: tablename, condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"_id", tablename)})
		}
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
		for i := 0; i < len(s.fields); i++ {
			var tablename string
			s.fields[i], tablename, _ = s.warpField(s.fields[i])
			if tablename != s.tableName {
				if !s.joinConditions.haveTable(tablename) {
					s.Joins(tablename)
				}
			}
		}
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

//DISTINCT XX
//DISTICT XXX.XXX AS aaa
//XXX.XXX AS aaa
func (s *search) warpField(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, " ") {
		if strings.Contains(field, "AS") {
			sp := strings.Split(field, " ")
			for i := 0; i < len(sp); i++ {
				if sp[i] == "AS" {
					sp[i-1], tablename, fieldname = s.warpFieldSingel(sp[i-1])
					warpStr = strings.Join(sp, " ")
					break
				}
			}
		} else {
			sp := strings.Split(field, " ")
			sp[len(sp)-1], tablename, fieldname = s.warpFieldSingel(sp[len(sp)-1])
			warpStr = strings.Join(sp, " ")
		}
	} else {
		return s.warpFieldSingel(field)
	}
	return
}

func (s *search) warpFieldSingel(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, ".") {
		sp := strings.Split(field, ".")
		tablename = sp[0]
		fieldname = sp[1]
		warpStr = strings.Replace(field, ".", ".`", 1) + "`"
	} else {
		tablename = s.tableName
		fieldname = field
		switch field {
		case "*", "COUNT(*)":
			warpStr = field
		default:
			warpStr = "`" + field + "`"
		}
	}
	return
}

//结果展示

func (s *search) Struct(v interface{}) {
	query, args := s.Parse()
	// s.db.Query(query, args...).Find(v)
	s.db.FindAll(v, append([]interface{}{query}, args...)...)
}

func (s *search) Count() int {
	var count int
	s.fields = []string{"COUNT(*)"}
	query, args := s.Parse()
	s.db.Query(query, args...).Find(&count)
	return count
}
