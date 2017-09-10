package crud

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

//
const (
	C      = "CREATE"
	CREATE = C
	R      = "READ"
	READ   = R
	U      = "UPDATE"
	UPDATE = U
	D      = "DELETE"
	DELET  = D
)

//DBColums 多列
var DBColums map[string]Column

func init() {
	DBColums = make(map[string]Column)
}

//Model 需要有一个将反射封装起来
type Model struct {
	fields []Field
}

//NewModel *Model
func NewModel(v interface{}) *Model {
	val := reflect.ValueOf(v)
	t := reflect.Indirect(val)
	fs := []Field{}
	for i := 0; i < t.NumField(); i++ {
		reflectField := t.Type().Field(i) //reflect.Type Field
		f := Field{
			name:       reflectField.Name,
			dbName:     ToDBName(reflectField.Name),
			value:      t.Field(i).Interface(),
			isBlank:    isBlank(t.Field(i)),
			iscRequire: reflectField.Tag.Get("c") == "require",
			isrRequire: reflectField.Tag.Get("r") == "require",
			isuRequire: reflectField.Tag.Get("u") == "require",
			isdRequire: reflectField.Tag.Get("d") == "require",
			isIgnore:   reflectField.Tag.Get("crud") == "ignore",
		}
		fs = append(fs, f)
	}
	return &Model{fields: fs}
}

//Fields 返回所有的字段
func (m *Model) Fields() []Field {
	return m.fields
}

//Field 表中的字段
type Field struct {
	name       string
	dbName     string
	value      interface{}
	isBlank    bool
	iscRequire bool
	isrRequire bool
	isuRequire bool
	isdRequire bool
	isIgnore   bool
}

//Name 对应的结构体字段名
func (f *Field) Name() string {
	return f.name
}

//DBName 结构体字段名对应的数据库名
func (f *Field) DBName() string {
	return f.dbName
}

//Value 值
func (f *Field) Value() interface{} {
	return f.value
}

//IsBlank 是否为空
func (f *Field) IsBlank() bool {
	return f.isBlank
}

//IsRequire 是否必须
func (f *Field) IsRequire(method string) bool {
	switch method {
	case C:
		return f.iscRequire
	case R:
		return f.isrRequire
	case U:
		return f.isuRequire
	case D:
		return f.isdRequire
	}
	return false
}

//IsIgnore 是否忽略此字段
func (f *Field) IsIgnore() bool {
	return f.isIgnore
}

// 获取结构体对应的数据库名
func getStructDBName(v reflect.Value) string {
	v = reflect.Indirect(v)
	var dbName string
	// 如果有DBName这个方法就调用这个获取表名，如果没有的话就通过toDBName获取表名
	dbNameFunc := v.MethodByName(DBName)
	if dbNameFunc.IsValid() {
		fmt.Println("is Valid")
		dbName = dbNameFunc.Call(nil)[0].String()
	} else {
		fmt.Println("NOT VALID")
		dbName = ToDBName(v.Type().Name())
	}
	return dbName
}

// 获取结构体ID
func getStructID(v reflect.Value) int64 {
	v = reflect.Indirect(v)
	rID := v.FieldByName("ID")
	if rID.IsValid() {
		return rID.Int()
	}
	return 0
}

// 检查反射的值是否为默认值，如果为默认值则默认为空值。
func isBlank(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return value.Len() == 0
	case reflect.Bool:
		return !value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return value.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return value.IsNil()
	}

	return reflect.DeepEqual(value.Interface(), reflect.Zero(value.Type()).Interface())
}

// 根据反射设置值，如果返回nil则说明参数错误。
func parseRequest(v interface{}, r *http.Request, method string) map[string]interface{} {
	r.FormValue("")
	m := make(map[string]interface{})
	for _, f := range NewModel(v).Fields() {
		_, ok := r.Form[f.DBName()]
		if f.IsRequire(method) && !ok {
			return nil
		}
		if ok {
			m[f.DBName()] = r.FormValue(f.DBName())
		}
	}
	for k := range r.Form {
		if strings.Contains(k, "_id") {
			m[k] = r.FormValue(k)
		}
	}
	return m
}
