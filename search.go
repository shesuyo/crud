package crud

import (
	"fmt"
	"strconv"
	"strings"
)

// JoinCon join条件
type JoinCon struct {
	TableName string
	Condition string
}

// JoinCons join条件slice
type JoinCons []JoinCon

// HaveTable join条件中是否已经添加了这张表的join
func (jc JoinCons) HaveTable(tableName string) bool {
	for _, v := range jc {
		if v.TableName == tableName {
			return true
		}
	}
	return false
}

// WhereCon where条件
type WhereCon struct {
	Query string
	Args  []interface{}
}

// Search 搜索结构体
type Search struct {
	table             *Table
	fields            []string
	tableName         string
	joinConditions    JoinCons
	whereConditions   []WhereCon
	orderbyConditions []string
	groupConditions   []string
	with              string
	having            string
	limit             interface{}
	offset            interface{}

	query string
	args  []interface{}
	raw   bool
}

// Clone 克隆一个当前结构体
func (s *Search) Clone() *Search {
	clone := *s
	return &clone
}

// Fields 需要查询的字段
func (s *Search) Fields(args ...string) *Search {
	if len(args) == 0 {
		return s
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "$C", "$c":
			args[i] = "COUNT(1) AS total"
		}
	}
	s.fields = append(s.fields, args...)
	return s
}

// Where where语法
func (s *Search) Where(query string, values ...interface{}) *Search {
	s.whereConditions = append(s.whereConditions, WhereCon{Query: query, Args: values})
	return s
}

// WhereID id = ?
func (s *Search) WhereID(id interface{}) *Search {
	s.whereConditions = append(s.whereConditions, WhereCon{Query: s.tableName + ".id = ?", Args: []interface{}{id}})
	return s
}

// In in语法
func (s *Search) In(field string, args ...interface{}) *Search {
	//in没有参数的话SQL就会报错
	if len(args) == 0 {
		return s
	}
	s.whereConditions = append(s.whereConditions, WhereCon{Query: fmt.Sprintf("%s IN (%s)", field, placeholder(len(args))), Args: args})
	return s
}

// NotIn not in 语法
func (s *Search) NotIn(field string, args ...interface{}) *Search {
	//not in没有参数的话SQL就会报错
	if len(args) == 0 {
		return s
	}
	s.whereConditions = append(s.whereConditions, WhereCon{Query: fmt.Sprintf("%s NOT IN (%s)", field, placeholder(len(args))), Args: args})
	return s
}

// Joins join语法，自动连表。
func (s *Search) Joins(tablename string, condition ...string) *Search {
	if len(condition) == 1 {
		s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: condition[0]})
	} else {
		if s.table.tableColumns[tablename].HaveColumn(s.tableName + "id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"id", s.tableName)})
		} else if s.table.tableColumns[tablename].HaveColumn(s.tableName + "_id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"_id", s.tableName)})
		} else if s.table.tableColumns[s.tableName].HaveColumn(tablename + "id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"id", tablename)})
		} else if s.table.tableColumns[s.tableName].HaveColumn(tablename + "_id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"_id", tablename)})
		}
	}
	return s
}

// OrderBy OrderBy 默认升序
func (s *Search) OrderBy(field string, isDESC ...bool) *Search {
	if len(isDESC) > 0 && isDESC[0] {
		s.orderbyConditions = append(s.orderbyConditions, field+" DESC")
	} else {
		s.orderbyConditions = append(s.orderbyConditions, field+" ASC")
	}
	return s
}

// TableName tableName
func (s *Search) TableName(name string) *Search {
	s.tableName = name
	return s
}

// Limit LIMIT ?
func (s *Search) Limit(limit interface{}) *Search {
	s.limit = limit
	return s
}

// Offset OFFSET ?
func (s *Search) Offset(offset interface{}) *Search {
	s.offset = offset
	return s
}

// Group GROUP BY
func (s *Search) Group(field ...string) *Search {
	s.groupConditions = append(s.groupConditions, field...)
	return s
}

// Parse 将各个条件整合成可以查询的SQL语句和参数
// SELECT COUNT(*) AS total,cid
// FROM report
// WHERE id > 1000000
// GROUP BY cid
// ORDER BY COUNT(*)
// LIMIT 1
// OFFSET 1
func (s *Search) Parse() (string, []interface{}) {
	if s.raw == true {
		return s.query, s.args
	}
	var (
		fields       string
		joins        string
		paddingwhere string
		wheres       []string
		orderby      string
		groupby      string
		limit        string
		offset       string
	)
	if s.table.tableColumns[s.tableName].HaveColumn(IsDeleted) {
		s.Where("is_deleted = ?", 0)
	}
	s.query = ""
	s.args = []interface{}{}
	if len(s.fields) == 0 {
		fields = "*"
	} else {
		for i := 0; i < len(s.fields); i++ {
			var tableName string
			s.fields[i], tableName, _ = s.warpField(s.fields[i])
			if tableName != s.tableName {
				if !s.joinConditions.HaveTable(tableName) {
					s.Joins(tableName)
				}
			}
		}
		fields = strings.Join(s.fields, ",")
	}
	for _, joincon := range s.joinConditions {
		joins += fmt.Sprintf(" LEFT JOIN %s ON %s", joincon.TableName, joincon.Condition)
	}
	for _, wherecon := range s.whereConditions {
		paddingwhere = " WHERE "
		wheres = append(wheres, wherecon.Query)
		s.args = append(s.args, wherecon.Args...)
	}
	if len(s.orderbyConditions) > 0 {
		orderby = " ORDER BY " + strings.Join(s.orderbyConditions, ",")
	}
	if len(s.groupConditions) > 0 {
		groupby = " GROUP BY " + strings.Join(s.groupConditions, ",")
	}
	if s.limit != nil {
		limit = " LIMIT ?"
		s.args = append(s.args, s.limit)
	}
	if s.offset != nil {
		offset = " OFFSET ?"
		s.args = append(s.args, s.offset)
	}
	s.query = fmt.Sprintf("SELECT %s FROM `%s`%s%s%s%s%s%s%s", fields, s.tableName, joins, paddingwhere, strings.Join(wheres, " AND "), groupby, orderby, limit, offset)
	// 如果table进行搜索了(table.RowsMap())，那么table下面所有的条件都会一直使用之前的搜索语句。
	// s.raw = true
	return s.query, s.args
}

// DISTINCT XX
// DISTICT XXX.XXX AS aaa
// XXX.XXX AS aaa
// COUNT(*) AS total
// tablename.*
// DATE_FORMAT(repair.createdtime,'%Y-%m-%d') AS dt
func (s *Search) warpField(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, " ") {
		if strings.Contains(field, "AS") {
			// XXX AS XXX
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

// warpFieldSingel field without space
// warp xxx OR xxx.xxx OR * OR COUNT(*) OR tablename.*
// 这里的都没有空格的
// 单个属性 id
// 表名.属性
// 表名.*
// COUNT(1)之类的函数
// DATE_FORMAT(repair.createdtime,'%Y-%m-%d')
func (s *Search) warpFieldSingel(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, ".") {
		sp := strings.Split(field, ".")
		tablename = sp[0]
		fieldname = sp[1]
		if tablename == "" {
			tablename = s.tableName
		}
		if fieldname == "" {
			fieldname = "*"
		}
		tablenameCombine := tablename
		fieldnameCombine := fieldname

		if !strings.Contains(tablename, "`") {
			tablenameCombine = "`" + tablename + "`"
		} else {
			tablename = strings.Replace(tablename, "`", "", -1)
		}

		if !strings.Contains(fieldname, "`") && fieldname != "*" {
			fieldnameCombine = "`" + fieldname + "`"
		} else {
			fieldname = strings.Replace(fieldname, "`", "", -1)
		}

		if s.table.DataBase.HaveTable(tablename) && s.table.DataBase.Table(tablename).HaveColumn(fieldname) {
			warpStr = tablenameCombine + "." + fieldnameCombine
		} else {
			warpStr = field
		}

	} else {
		// 如果没有.
		tablename = s.tableName
		fieldname = field
		warpStr = field
		cols := s.table.DataBase.getColumns(tablename)
		for _, col := range cols {
			if col.Name == field {
				warpStr = "`" + tablename + "`.`" + field + "`"
				break
			}
		}
	}
	return
}

//结果展示

// RawMap RawMap
func (s *Search) RawMap() RowMap {
	return s.RowMap()
}

// RawsMap RawsMap
func (s *Search) RawsMap() RowsMap {
	return s.RowsMap()
}

// RawsMapInterface RawsMapInterface
func (s *Search) RawsMapInterface() RowsMapInterface {
	return s.RowsMapInterface()
}

// RowMap RowMap
func (s *Search) RowMap() RowMap {
	nb := (*s).Clone().Limit(1)
	query, args := nb.Parse()
	return s.table.Query(query, args...).RowMap()
}

// Explain explian sql
func (s *Search) Explain(debug bool) Explain {
	query, args := s.Parse()
	r := s.table.Query("EXPLAIN "+query, args...).RowMap()
	if debug {
		fmt.Println(query)
		fmt.Println(args)
		fmt.Println(getFullSQL(query, args...))
	}
	e := Explain{
		ID:           r.Int("id"),
		SelectType:   r["select_type"],
		Table:        r["table"],
		Partitions:   r["partitions"],
		Type:         r["type"],
		PossibleKeys: r["possible_keys"],
		Key:          r["key"],
		KeyLen:       r.Int("key_len"),
		Ref:          r["ref"],
		Rows:         r.Int("rows"),
		Filtered:     r.Int("filtered"),
		Extra:        r["extra"],
	}
	return e
}

// func (s *Search) String() string {
// 	return s.table.Query(s.Parse()).String()
// }

// SQLRows SQLRows
func (s *Search) SQLRows() *SQLRows {
	query, args := s.Parse()
	return s.table.Query(query, args...)
}

// RowsMap RowsMap
func (s *Search) RowsMap() RowsMap {
	query, args := s.Parse()
	return s.table.Query(query, args...).RowsMap()
}

// RowMapInterface RowMapInterface
func (s *Search) RowMapInterface() RowMapInterface {
	query, args := s.Parse()
	return s.table.Query(query, args...).RowMapInterface()
}

// RowsMapInterface RowsMapInterface
func (s *Search) RowsMapInterface() RowsMapInterface {
	query, args := s.Parse()
	return s.table.Query(query, args...).RowsMapInterface()
}

// DoubleSlice DoubleSlice
func (s *Search) DoubleSlice() (map[string]int, [][]string) {
	query, args := s.Parse()
	return s.table.Query(query, args...).DoubleSlice()
}

// Int 如果指定字段，则返回指定字段的int值，否则返回第一个字段作为int值返回。
func (s *Search) Int(args ...string) int {
	row := s.RowMap()
	if len(args) == 0 {
		for _, v := range row {
			i, _ := strconv.Atoi(v)
			return i
		}
	} else {
		i, _ := strconv.Atoi(row[args[0]])
		return i
	}
	return 0
}

// String like int
func (s *Search) String(args ...string) string {
	row := s.RowMap()
	if len(args) == 0 {
		for _, v := range row {
			return v
		}
	} else {
		return row[args[0]]
	}
	return ""
}

// Bool Bool
func (s *Search) Bool(args ...string) bool {
	row := s.RowMap()
	return row.Bool(args...)
}

// Finds 将查询的结构放入到结构体当中
func (s *Search) Finds(v interface{}) {
	query, args := s.Parse()
	s.table.FindAll(v, append([]interface{}{query}, args...)...)
}

// //Count 计算这次查询结果的个数
// func (s *Search) Count() int {
// 	var count int
// 	s.fields = []string{"COUNT(*)"}
// 	query, args := s.Parse()
// 	s.table.Query(query, args...).Find(&count)
// 	return count
// }
