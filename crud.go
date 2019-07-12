package crud

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql" //mysql driver
)

// 变量
var (
	TimeFormat = "2006-01-02 15:04:05"

	//错误
	ErrExec = errors.New("执行错误")
	ErrArgs = errors.New("参数错误")

	ErrInsertRepeat = errors.New("重复插入")
	ErrSQLSyntaxc   = errors.New("SQL语法错误")
	ErrInsertData   = errors.New("插入数据库异常")
	ErrNoUpdateKey  = errors.New("没有更新主键")

	ErrMustBeAddr     = errors.New("必须为值引用")
	ErrMustBeSlice    = errors.New("必须为Slice")
	ErrMustNeedID     = errors.New("必须要有ID")
	ErrNotSupportType = errors.New("不支持类型")
)

// Render 用于对接http.HandleFunc直接调用CRUD
type Render func(w http.ResponseWriter, err error, data ...interface{})

// DataBase 数据库链接
type DataBase struct {
	debug bool

	Schema         string //数据库表名
	tableColumns   map[string]Columns
	dataSourceName string
	db             *sql.DB

	mm *sync.Mutex // 用于getColumns的写锁

	render Render //crud本身不渲染数据，通过其他地方传入一个渲染的函数，然后渲染都是那边处理。
}

// NewDataBase 创建一个新的数据库链接
func NewDataBase(dataSourceName string, render ...Render) (*DataBase, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	crud := &DataBase{
		debug:          false,
		tableColumns:   make(map[string]Columns),
		dataSourceName: dataSourceName,
		db:             db,
		mm:             new(sync.Mutex),
		render: func(w http.ResponseWriter, err error, data ...interface{}) {
			if len(render) == 1 {
				if render[0] != nil {
					render[0](w, err, data...)
				}
			}
		},
	}

	crud.Schema = crud.Query("SELECT DATABASE()").String()
	if crud.Schema == "" {
		log.Println("FBI WARNING: 这是一个没有选择数据库的链接。")
	}
	tables := crud.Query("SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE,IS_NULLABLE FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = ?", crud.Schema).RowsMap().MapIndexs("TABLE_NAME")

	for tableName, cols := range tables {
		cm := make(map[string]Column)
		for _, v := range cols {
			cm[v["COLUMN_NAME"]] = Column{
				Schema:     v["TABLE_SCHEMA"],
				Table:      v["TABLE_NAME"],
				Name:       v["COLUMN_NAME"],
				Comment:    v["COLUMN_COMMENT"],
				ColumnType: v["COLUMN_TYPE"],
				DataType:   v["DATA_TYPE"],
				IsNullAble: v["IS_NULLABLE"] == "YES",
			}
		}
		crud.tableColumns[tableName] = cm
	}

	return crud, nil
}

/*
	CRUD table
*/

// ExecSuccessRender 渲染成功模板
func (db *DataBase) ExecSuccessRender(w http.ResponseWriter) {
	db.render(w, nil, nil)
}

func (db *DataBase) argsErrorRender(w http.ResponseWriter) {
	db.render(w, ErrArgs)
}

func (db *DataBase) execErrorRender(w http.ResponseWriter) {
	db.render(w, ErrExec)
}

func (db *DataBase) dataRender(w http.ResponseWriter, data interface{}) {
	db.render(w, nil, data)
}

/*
	CRUD colums table
*/

// HaveTable 是否有这张表
func (db *DataBase) HaveTable(tablename string) bool {
	return db.haveTablename(tablename)
}

func (db *DataBase) haveTablename(tableName string) bool {
	_, ok := db.tableColumns[tableName]
	return ok
}

// 获取表中所有列名
func (db *DataBase) getColumns(tableName string) Columns {
	names, ok := db.tableColumns[tableName]
	if ok {
		return names
	}
	rows := db.Query("SELECT COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE,IS_NULLABLE FROM information_schema.`COLUMNS` WHERE table_name= ? ", tableName).RowsMap()
	cols := make(map[string]Column)
	for _, v := range rows {
		cols[v["COLUMN_NAME"]] = Column{
			Name:       v["COLUMN_NAME"],
			Comment:    v["COLUMN_COMMENT"],
			ColumnType: v["COLUMN_TYPE"],
			DataType:   v["DATA_TYPE"],
			IsNullAble: v["IS_NULLABLE"] == "YES",
		}
		dbcM.Lock()
		DBColums[v["COLUMN_NAME"]] = cols[v["COLUMN_NAME"]]
		dbcM.Unlock()
	}
	db.mm.Lock()
	db.tableColumns[tableName] = cols
	db.mm.Unlock()
	return cols
}

// Table 返回一个Table
func (db *DataBase) Table(tableName string) *Table {
	if !db.HaveTable(tableName) {
		// fmt.Println("FBI WARNING:表" + tableName + "不存在！")
	}
	table := new(Table)
	table.DataBase = db
	table.tableName = tableName
	table.Search = &Search{
		table:     table,
		tableName: tableName,
	}
	table.Columns = db.tableColumns[tableName]
	return table
}

/*
	CRUD debug
*/

// Debug 是否开启debug功能 true为开启
func (db *DataBase) Debug(isDebug bool) *DataBase {
	db.debug = isDebug
	return db
}

// X 用于DEBUG
func (*DataBase) X(args ...interface{}) {
	fmt.Println("[DEBUG]", args)
}

// Log 打印日志
func (db *DataBase) Log(args ...interface{}) {
	if db.debug {
		db.log(args...)
	}
}

// LogSQL 会将sql语句中的?替换成相应的参数，让DEBUG的时候可以直接复制SQL语句去使用。
func (db *DataBase) LogSQL(sql string, args ...interface{}) {
	if db.debug {
		db.log(getFullSQL(sql, args...))
	}
}

func (db *DataBase) log(args ...interface{}) {
	log.Println(args...)
}

func getFullSQL(sql string, args ...interface{}) string {
	for _, arg := range args {
		sql = strings.Replace(sql, "?", fmt.Sprintf("'%v'", arg), 1)
	}
	return sql
}

// 如果发生了异常就打印调用栈。
func (db *DataBase) stack(err error, sql string, args ...interface{}) {
	buf := make([]byte, 1<<10)
	runtime.Stack(buf, true)
	log.Printf("%s\n%s\n%s\n", err.Error(), getFullSQL(sql, args...), buf)
}

// RowSQL Query alias
func (db *DataBase) RowSQL(sql string, args ...interface{}) *SQLRows {
	return db.Query(sql, args...)
}

/*
	CRUD 查询
*/

// Query 用于底层查询，一般是SELECT语句
func (db *DataBase) Query(sql string, args ...interface{}) *SQLRows {
	db.LogSQL(sql, args...)
	rows, err := db.DB().Query(sql, args...)

	if err != nil {
		db.stack(err, sql, args...)
	}
	return &SQLRows{rows: rows, err: err}
}

// Exec 用于底层执行，一般是INSERT INTO、DELETE、UPDATE。
func (db *DataBase) Exec(sql string, args ...interface{}) sql.Result {
	db.LogSQL(sql, args...)
	ret, err := db.DB().Exec(sql, args...)
	if err != nil {
		db.stack(err, sql, args...)
	}
	return ret
}

// DB 返回一个DB链接，查询后一定要关闭col，而不能关闭*sql.DB。
func (db *DataBase) DB() *sql.DB {
	return db.db
}

// Create 根据相应单个结构体进行创建
func (db *DataBase) Create(obj interface{}) (int64, error) {
	//一定要是地址
	//需要检查Before函数
	//需要按需转换成map(考虑ignore)
	//需要检查After函数
	//TODO 一次性创建整个嵌套结构体
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return 0, ErrMustBeAddr
	}
	beforeFunc := v.MethodByName(BeforeCreate)
	afterFunc := v.MethodByName(AfterCreate)
	tableName := getStructDBName(v)

	// 这里的处理应该是有才处理，没有不管。
	if beforeFunc.IsValid() {
		vals := beforeFunc.Call(nil)
		if len(vals) == 1 {
			if err, ok := vals[0].Interface().(error); ok {
				if err != nil {
					return 0, err
				}
			}
		}
	}
	m := structToMap(v)
	table := db.Table(tableName)
	for k, v := range m {
		if k == "id" && v == "" {
			delete(m, "id")
		}
		if table.Columns[k].DataType == "datetime" && v == "" {
			delete(m, k)
		}
	}
	id, err := table.Create(m)

	rID := v.Elem().FieldByName("ID")
	if rID.IsValid() {
		rID.SetInt(id)
	}

	if afterFunc.IsValid() {
		afterFunc.Call(nil)
	}
	return id, err
}

// Creates 根据相应多个结构体进行创建
func (db *DataBase) Creates(objs interface{}) ([]int64, error) {
	ids := []int64{}
	v := reflect.ValueOf(objs)
	if v.Kind() != reflect.Ptr {
		return ids, ErrMustBeAddr
	}
	if v.Elem().Kind() != reflect.Slice {
		return ids, ErrMustBeSlice
	}

	for i, num := 0, v.Elem().Len(); i < num; i++ {
		id, err := db.Create(v.Elem().Index(i).Addr().Interface())
		if err != nil {
			return ids, err
		}

		ids = append(ids, id)
	}

	return ids, nil
}

// Update Update
func (db *DataBase) Update(obj interface{}) error {
	//根据ID进行Update
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return ErrMustBeAddr
	}
	beforeFunc := v.MethodByName(BeforeUpdate)
	afterFunc := v.MethodByName(AfterUpdate)
	if beforeFunc.IsValid() {
		beforeFunc.Call(nil)
	}
	tableName := getStructDBName(v)
	m := structToMap(v)
	err := db.Table(tableName).Update(m)

	if err != nil {
		return err
	}
	if afterFunc.IsValid() {
		afterFunc.Call(nil)
	}
	return nil
}

// Updates Updates
func (db *DataBase) Updates(objs interface{}) error {
	v := reflect.ValueOf(objs)
	if v.Kind() != reflect.Ptr {
		return ErrMustBeAddr
	}
	if v.Elem().Kind() != reflect.Slice {
		return ErrMustBeSlice
	}

	for i, num := 0, v.Elem().Len(); i < num; i++ {
		err := db.Update(v.Elem().Index(i).Addr().Interface())
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete Delete
func (db *DataBase) Delete(obj interface{}) (int64, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return 0, ErrMustBeAddr
	}
	beforeFunc := v.MethodByName(BeforeDelete)
	afterFunc := v.MethodByName(AfterDelete)
	if beforeFunc.IsValid() {
		beforeFunc.Call(nil)
	}
	id := getStructID(v)
	if id == 0 {
		return 0, ErrMustNeedID
	}
	tableName := getStructDBName(v)

	count, err := db.Table(tableName).Delete(map[string]interface{}{"id": id})
	if afterFunc.IsValid() {
		afterFunc.Call(nil)
	}
	return count, err
}

// Deletes Deletes
func (db *DataBase) Deletes(objs interface{}) (int64, error) {
	var affCount int64
	v := reflect.ValueOf(objs)
	if v.Kind() != reflect.Ptr {
		return 0, ErrMustBeAddr
	}
	if v.Elem().Kind() != reflect.Slice {
		return 0, ErrMustBeSlice
	}

	for i, num := 0, v.Elem().Len(); i < num; i++ {
		aff, err := db.Delete(v.Elem().Index(i).Addr().Interface())
		affCount += aff
		if err != nil {
			return affCount, err
		}
	}
	return 0, nil
}

// FormCreate 创建，表单创建。
func (db *DataBase) FormCreate(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(reflect.ValueOf(v))
	m := parseRequest(v, r, C)
	if m == nil || len(m) == 0 {
		db.argsErrorRender(w)
		return
	}
	id, err := db.Table(tableName).Create(m)
	if err != nil {
		db.execErrorRender(w)
		return
	}
	m["id"] = id
	delete(m, IsDeleted)
	db.dataRender(w, m)
}

// FormRead 表单查找
/*
	查找
	id = 1
	id = 1  AND hospital_id = 1

	CRUD FormRead -> table Read
*/
func (db *DataBase) FormRead(v interface{}, w http.ResponseWriter, r *http.Request) {
	//	这里传进来的参数一定是要有用的参数，如果是没有用的参数被传进来了，那么会报参数错误，或者显示执行成功数据会乱。
	//	这里处理last_XXX
	//	处理翻页的问题
	//	首先判断这个里面有没有这个字段
	m := parseRequest(v, r, R)

	tableName := getStructDBName(reflect.ValueOf(v))
	data := db.Table(tableName).Reads(m)
	db.dataRender(w, data)
}

// FormUpdate 表单更新
func (db *DataBase) FormUpdate(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(reflect.ValueOf(v))
	m := parseRequest(v, r, R)
	if m == nil || len(m) == 0 {
		db.argsErrorRender(w)
		return
	}
	err := db.Table(tableName).Update(m)
	if err != nil {
		db.execErrorRender(w)
		return
	}
	db.ExecSuccessRender(w)
}

// FormDelete 表单删除
func (db *DataBase) FormDelete(v interface{}, w http.ResponseWriter, r *http.Request) {
	tableName := getStructDBName(reflect.ValueOf(v))
	m := parseRequest(v, r, R)
	if m == nil || len(m) == 0 {
		db.argsErrorRender(w)
		return
	}
	_, err := db.Table(tableName).Delete(m)
	if err != nil {
		db.execErrorRender(w)
		return
	}
	db.ExecSuccessRender(w)
}

// Find 将查找数据放到结构体里面
// 如果不传条件则是查找所有人
// Read Find Select
func (db *DataBase) Find(obj interface{}, args ...interface{}) error {
	var (
		v = reflect.ValueOf(obj)

		tableName = ""
		elem      = v.Elem()
		where     = " WHERE 1 "

		rawSqlflag = false
	)

	if v.Kind() != reflect.Ptr {
		return ErrMustBeAddr
	}

	if len(args) > 0 {
		if sql, ok := args[0].(string); ok {
			if strings.Contains(sql, "SELECT") {
				rawSqlflag = true
				err := db.Query(sql, args[1:]...).Find(obj)
				if err != nil {
					return err
				}
			}
		}
	}

	if !rawSqlflag {
		if elem.Kind() == reflect.Slice {
			tableName = getStructDBName(reflect.New(elem.Type().Elem()))
		} else {
			tableName = getStructDBName(elem)
		}

		if len(args) == 1 {
			where += " AND id = ? "
			args = append(args, args[0])
		} else if len(args) > 1 {
			where += "AND " + args[0].(string)
		} else {
			//avoid args[1:]... bounds out of range
			args = append(args, nil)

			//如果没有传参数，那么参数就在结构体本身。（只支持ID,而且是结构体的时候）
			if elem.Kind() == reflect.Struct {
				rID := elem.FieldByName("ID")
				if rID.IsValid() {
					rIDInt64 := rID.Int()
					if rIDInt64 != 0 {
						where += " AND id = ? "
						args = append(args, rIDInt64)
					}
				}
			}
		}

		if db.tableColumns[tableName].HaveColumn(IsDeleted) {
			where += " AND is_deleted = 0"
		}

		err := db.Query(fmt.Sprintf("SELECT * FROM `%s` %s", tableName, where), args[1:]...).Find(obj)
		if err != nil {
			return err
		}
	}

	switch elem.Kind() {
	case reflect.Slice:
		for i, num := 0, elem.Len(); i < num; i++ {
			afterFunc := elem.Index(i).Addr().MethodByName("AfterFind")
			if !afterFunc.IsValid() {
				return nil
			}
			afterFunc.Call(nil)
		}
	case reflect.Struct:
		afterFunc := v.MethodByName("AfterFind")
		if afterFunc.IsValid() {
			afterFunc.Call(nil)
		}
	}

	return nil
}

// connection 找出两张表之间的关联
/*
	根据belong查询master
	master是要查找的，belong是已知的。
*/
func (db *DataBase) connection(target string, got reflect.Value) ([]interface{}, bool) {
	//"SELECT `master`.* FROM `master` WHERE `belong_id` = ? ", belongID
	//"SELECT `master`.* FROM `master` LEFT JOIN `belong` ON `master`.id = `belong`.master_id WHERE `belong`.id = ?"
	//"SELECT `master`.* FROM `master` LEFT JOIN `belong` ON `master`.belong_id = `belong`.id WHERE `belong`.id = ?"
	//"SELECT `master`.* FROM `master` LEFT JOIN `master_belong` ON `master_belong`.master_id = `master`.id WHERE `master_belong`.belong_id = ?", belongID
	//"SELECT `master`.* FROM `master` LEFT JOIN `belong_master` ON `belong_master`.master_id = `master`.id WHERE `belong_master`.belong_id = ?", belongID
	// 首先实现正常的逻辑，然后再进行所有逻辑的判断。

	ttn := target                      //target table name
	gtn := ToDBName(got.Type().Name()) // got table name

	//fmt.Println(ttn, gtn)

	if db.tableColumns[gtn].HaveColumn(ttn + "_id") {
		// got: question_option question_id
		// target: question
		// select * from question where id = question_option.question_id
		//return db.RowSQL(fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE %s = ?", gtn, gtn, "id"), got.FieldByName(ttn+"_id").Interface())
		return []interface{}{fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE %s = ?", gtn, gtn, "id"), got.FieldByName(ToStructName(ttn + "_id")).Interface()}, true
	}

	if db.tableColumns[ttn].HaveColumn(gtn + "_id") {
		//got: question
		//target:question_options
		//select * from question_options where question.options.question_id = question.id
		//		return db.RowSQL(fmt.Sprintf("SELECT * FROM `%s` WHERE %s = ?", ttn, gtn+"_id"), got.FieldByName("id").Interface())
		return []interface{}{fmt.Sprintf("SELECT * FROM `%s` WHERE %s = ?", ttn, gtn+"_id"), got.FieldByName("ID").Interface()}, true
	}

	//group_section
	//got: group
	//target: section
	//SELECT section.* FROM section LEFT JOIN group_section ON group_section.section_id = section.id WHERE group_section.group_id = group.id

	ctn := ""
	if db.haveTablename(ttn + "_" + gtn) {
		ctn = ttn + "_" + gtn
	}

	if db.haveTablename(gtn + "_" + ttn) {
		ctn = gtn + "_" + ttn
	}

	if ctn != "" {
		if db.tableColumns[ctn].HaveColumn(gtn+"_id") && db.tableColumns[ctn].HaveColumn(ttn+"_id") {
			//			return db.RowSQL(fmt.Sprintf("SELECT `%s`.* FROM `%s` LEFT JOIN %s ON %s.%s = %s.%s WHERE %s.%s = ?", ttn, ttn, ctn, ctn, ttn+"_id", ttn, "id", ctn, gtn+"_id"),
			//				got.FieldByName("id").Interface())
			return []interface{}{fmt.Sprintf("SELECT `%s`.* FROM `%s` LEFT JOIN %s ON %s.%s = %s.%s WHERE %s.%s = ?", ttn, ttn, ctn, ctn, ttn+"_id", ttn, "id", ctn, gtn+"_id"),
				got.FieldByName("ID").Interface()}, true
		}
	}

	return []interface{}{}, false
}

// FindAll 在需要的时候将自动查询结构体子结构体
func (db *DataBase) FindAll(v interface{}, args ...interface{}) error {
	if err := db.Find(v, args...); err != nil {
		return err
	}
	//首先查找字段，然后再查找结构体和Slice
	/*
		首先实现结构体
		//不处理指针

	*/
	rv := reflect.ValueOf(v).Elem()
	switch rv.Kind() {
	case reflect.Struct:
		db.setStructField(rv)
	case reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			db.setStructField(rv.Index(i))
		}
	default:
		return ErrNotSupportType
	}
	return nil
}

func (db *DataBase) setStructField(rv reflect.Value) {
	for i := 0; i < rv.NumField(); i++ {
		if rv.Field(i).Kind() == reflect.Struct {
			con, ok := db.connection(ToDBName(rv.Field(i).Type().Name()), rv)
			if ok {
				db.FindAll(rv.Field(i).Addr().Interface(), con...)
			}
		}
		if rv.Field(i).Kind() == reflect.Slice {
			con, ok := db.connection(ToDBName(rv.Field(i).Type().Elem().Name()), rv)
			if ok {
				db.FindAll(rv.Field(i).Addr().Interface(), con...)
			}
		}
	}
}
