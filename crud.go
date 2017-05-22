package crud

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ExecError = errors.New("执行错误")
	ArgsError = errors.New("参数错误")
)

type CRUDRender func(w http.ResponseWriter, err error, data ...interface{})

type Columns map[string]Column

func (cs Columns) HaveColumn(columnName string) bool {
	if cs == nil {
		return false
	}
	_, ok := cs[columnName]
	return ok
}

type Column struct {
	Name       string
	Comment    string
	ColumnType string
	DataType   string
}

type CRUD struct {
	debug bool

	tableColumns   map[string]Columns
	dataSourceName string
	render         CRUDRender //crud本身不渲染数据，通过其他地方传入一个渲染的函数，然后渲染都是那边处理。
}

func NewCRUD(dataSourceName string, render CRUDRender) *CRUD {
	crud := &CRUD{
		debug:          false,
		tableColumns:   make(map[string]Columns),
		dataSourceName: dataSourceName,
		render:         render,
	}

	for _, tbm := range crud.RowSQL("SHOW TABLES").RawsMap() {
		for _, v := range tbm {
			crud.getColums(v)
		}
	}
	return crud
}

func (this *CRUD) X(args ...interface{}) {
	fmt.Println("[DEBUG]", args)
}

func (this *CRUD) ExecSuccessRender(w http.ResponseWriter) {
	this.render(w, nil, nil)
}

func (this *CRUD) argsErrorRender(w http.ResponseWriter) {
	this.render(w, ArgsError)
}

func (this *CRUD) execErrorRender(w http.ResponseWriter) {
	this.render(w, ExecError)
}

func (this *CRUD) dataRender(w http.ResponseWriter, data interface{}) {
	this.render(w, nil, data)
}

// 是否有这张表名
func (this *CRUD) haveTablename(tableName string) bool {
	_, ok := this.tableColumns[tableName]
	return ok
}

//获取表中所有列名
func (this *CRUD) getColums(tablename string) Columns {
	names, ok := this.tableColumns[tablename]
	if ok {
		return names
	}
	raws := this.RowSQL("SELECT COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE FROM information_schema.`COLUMNS` WHERE table_name= ? ", tablename).RawsMap()
	cols := make(map[string]Column)
	for _, v := range raws {
		cols[v["COLUMN_NAME"]] = Column{Name: v["COLUMN_NAME"], Comment: v["COLUMN_COMMENT"], ColumnType: v["COLUMN_TYPE"], DataType: v["DATA_TYPE"]}
		DBColums[v["COLUMN_NAME"]] = cols[v["COLUMN_NAME"]]
	}
	this.tableColumns[tablename] = cols
	return cols
}

func (this *CRUD) Debug(isDebug bool) *CRUD {
	this.debug = isDebug
	return this
}

func (this *CRUD) Log(args ...interface{}) {
	if this.debug {
		fmt.Println(args...)
	}
}

func (this *CRUD) RowSQL(sql string, args ...interface{}) *SQLRows {
	db, err := this.DB()
	defer db.Close()
	if err != nil {
		this.Log("[ERROR]", err)
		return &SQLRows{}
	}
	this.Log(sql, args)
	rows, err := db.Query(sql, args...)
	return &SQLRows{rows: rows, err: err}
}
func (this *CRUD) Exec(sql string, args ...interface{}) *SQLResult {
	db, err := this.DB()
	defer db.Close()
	if err != nil {
		this.Log("[ERROR]", err)
		return &SQLResult{}
	}
	this.Log(sql, args)
	ret, err := db.Exec(sql, args...)
	return &SQLResult{ret: ret, err: err}
}

func (this *CRUD) DB() (*sql.DB, error) {
	// TODO 进行短连接和连接池的效率比较
	return sql.Open("mysql", this.dataSourceName)
}

func (this *CRUD) Create(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(v)
	m := parseRequest(v, r, C)
	if m == nil || len(m) == 0 {
		this.argsErrorRender(w)
		return
	}
	names := []string{}
	values := []string{}
	args := []interface{}{}
	cols := this.getColums(tableName)
	if cols.HaveColumn("create_at") {
		time.Now().Format("2006-01-02 15:04:05")
	}
	if cols.HaveColumn("is_delete") {
		m["is_delete"] = 0
	}
	for k, v := range m {
		names = append(names, "`"+k+"`")
		values = append(values, "?")
		args = append(args, v)
	}
	ret := this.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(names, ","), strings.Join(values, ",")), args...)
	id, err := ret.ID()
	if err != nil {
		this.execErrorRender(w)
		return
	}
	m["id"] = id
	this.dataRender(w, m)
}

/*
	id = 1
	id = 1  AND hospital_id = 1

*/
func (this *CRUD) Read(v interface{}, w http.ResponseWriter, r *http.Request) {
	//	这里传进来的参数一定是要有用的参数，如果是没有用的参数被传进来了，那么会报参数错误，或者显示执行成功数据会乱。
	//	这里处理last_XXX
	//	处理翻页的问题
	//	首先判断这个里面有没有这个字段
	m := parseRequest(v, r, R)
	if m == nil || len(m) == 0 {
		this.argsErrorRender(w)
		return
	}
	//	看一下是不是其他表关联查找
	tableName := getStructDBName(v)
	cols := this.getColums(tableName)
	ctn := "" //combine table name
	//fk := ""
	//var fkv interface{}
	for k, _ := range m {
		this.X("检查表" + k)
		if !cols.HaveColumn(k) {
			if strings.Contains(k, "_id") {
				atn := strings.TrimRight(k, "_id") //another table name
				tmptn := atn + "_" + tableName
				this.X("检查表" + tmptn)
				if this.haveTablename(tmptn) {
					if this.tableColumns[tmptn].HaveColumn(k) {
						ctn = tmptn
					}
				}
				this.X("检查表" + tmptn)
				tmptn = tableName + "_" + atn
				if this.haveTablename(tmptn) {
					if this.tableColumns[tmptn].HaveColumn(k) {
						ctn = tmptn
					}
				}
				if ctn == "" {
					this.argsErrorRender(w)
					return
				} else {
					//fk = k
					//fkv = m[fk]
				}
			}
		}
	}

	if ctn == "" {
		ks, vs := ksvs(m, " = ? ")
		data := this.RowSQL(fmt.Sprintf("SELECT * FROM `%s` WHERE %s", tableName, strings.Join(ks, "AND")), vs...).RawsMapInterface()
		this.dataRender(w, data)
	} else {
		ks, vs := ksvs(m, " = ? ")
		//SELECT `section`.* FROM `group_section` LEFT JOIN section ON group_section.section_id = section.id WHERE group_id = 1
		data := this.RowSQL(fmt.Sprintf("SELECT `%s`.* FROM `%s` LEFT JOIN `%s` ON `%s`.`%s` = `%s`.`%s` WHERE %s", tableName, ctn, tableName, ctn, tableName+"_id", tableName, "id", strings.Join(ks, "AND")), vs...).RawsMapInterface()
		this.dataRender(w, data)
	}
}

func (this *CRUD) Update(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(v)
	m := parseRequest(v, r, R)
	if m == nil || len(m) == 0 {
		this.argsErrorRender(w)
		return
	}
	//	UPDATE task SET name = ? WHERE id = 3;
	//	现在只支持根据ID进行更新
	id := m["id"]
	delete(m, "id")
	ks, vs := ksvs(m, " = ? ")
	vs = append(vs, id)
	_, err := this.Exec(fmt.Sprintf("UPDATE `%s` SET %s WHERE %s = ?", tableName, strings.Join(ks, "AND"), "id"), vs...).Effected()
	if err != nil {
		this.execErrorRender(w)
		return
	}
	this.ExecSuccessRender(w)
}
func (this *CRUD) Delete(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(v)
	m := parseRequest(v, r, R)
	if m == nil || len(m) == 0 {
		this.argsErrorRender(w)
		return
	}
	//	现在只支持根据ID进行删除
	ks, vs := ksvs(m, " = ? ")
	_, err := this.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s ", tableName, strings.Join(ks, "AND")), vs...).Effected()
	if err != nil {
		this.execErrorRender(w)
		return
	}
	this.ExecSuccessRender(w)
}

//将查找数据放到结构体里面
func (this *CRUD) Find(v interface{}, args ...interface{}) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		fmt.Println("Must Need Addr")
		return
	}

	tableName := ""
	if rv.Elem().Kind() == reflect.Slice {
		tableName = ToDBName(rv.Elem().Type().Elem().Name())
	} else {
		tableName = ToDBName(rv.Type().Elem().Name())
	}

	// 如果传入

	if len(args) == 1 {
		this.RowSQL(fmt.Sprintf("SELECT * FROM `%s` WHERE `id` = ?", tableName), args[0]).Find(v)
	} else if len(args) > 1 {
		this.RowSQL(fmt.Sprintf("SELECT * FROM `%s` WHERE %s", tableName, args[0]), args[1:]...).Find(v)
	} else {
		this.RowSQL(fmt.Sprintf("SELECT * FROM `%s`", tableName)).Find(v)
	}
}

//在需要的时候将自动查询结构体子结构体
func (this *CRUD) FindAll(v interface{}, args ...interface{}) {
	this.Find(v, args...)
	//然后再查找
}
