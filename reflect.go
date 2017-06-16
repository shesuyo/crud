package crud

import (
	"net/http"
	"reflect"
	"strings"
)

var (
	C      = "CREATE"
	CREATE = C
	R      = "READ"
	READ   = R
	U      = "UPDATE"
	UPDATE = U
	D      = "DELETE"
	DELET  = D
)

var DBColums map[string]Column

func init() {
	DBColums = make(map[string]Column)
}

//需要有一个将反射封装起来
type Model struct {
	fields []Field
}

func NewModel(v interface{}) *Model {
	val := reflect.ValueOf(v)
	t := reflect.Indirect(val)
	fs := []Field{}
	for i := 0; i < t.NumField(); i++ {
		f := Field{
			name:       t.Type().Field(i).Name,
			db_name:    ToDBName(t.Type().Field(i).Name),
			value:      t.Field(i).Interface(),
			isBlank:    isBlank(t.Field(i)),
			iscRequire: t.Type().Field(i).Tag.Get("c") == "require",
			isrRequire: t.Type().Field(i).Tag.Get("r") == "require",
			isuRequire: t.Type().Field(i).Tag.Get("u") == "require",
			isdRequire: t.Type().Field(i).Tag.Get("d") == "require",
		}
		fs = append(fs, f)
	}
	return &Model{fields: fs}
}

func (m *Model) Fields() []Field {
	return m.fields
}

type Field struct {
	name       string
	db_name    string
	value      interface{}
	isBlank    bool
	iscRequire bool
	isrRequire bool
	isuRequire bool
	isdRequire bool
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) DBName() string {
	return f.db_name
}

func (f *Field) Value() interface{} {
	return f.value
}

func (f *Field) IsBlank() bool {
	return f.isBlank
}

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

// 获取结构体对应的数据库名
func getStructDBName(v interface{}) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		//如果是传地址的话，则先获取到元素再获取名字。
		return ToDBName(t.Elem().Name())
	}
	return ToDBName(t.Name())
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
