package crud

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	fullTitles         = []string{"API", "CPU", "CSS", "CID", "DNS", "EOF", "EPC", "GUID", "HTML", "HTTP", "HTTPS", "ID", "UID", "IP", "JSON", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SSH", "TLS", "TTL", "UI", "UID", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS", "PY"}
	fullTitlesReplacer *strings.Replacer
	structNameMap      map[string]string
	//m和rm公用同一个
	dbNameMap = NewMapStringString()

	placeholders = []string{"", "?", "?,?", "?,?,?", "?,?,?,?", "?,?,?,?,?", "?,?,?,?,?,?", "?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?,?,?"}
)

// SafeMapStringString 安全的map[string]string
type SafeMapStringString struct {
	m  map[string]string
	mu sync.RWMutex
}

// Get Get
func (safe *SafeMapStringString) Get(key string) (string, bool) {
	safe.mu.RLock()
	val, ok := safe.m[key]
	safe.mu.RUnlock()
	return val, ok
}

// Set Set
func (safe *SafeMapStringString) Set(key, val string) {
	safe.mu.Lock()
	safe.m[key] = val
	safe.mu.Unlock()
}

// NewMapStringString 返回一个安全的map[string]string
func NewMapStringString() *SafeMapStringString {
	safe := new(SafeMapStringString)
	safe.m = make(map[string]string)
	return safe
}

func init() {
	{
		var oldnew []string
		for _, title := range fullTitles {
			oldnew = append(oldnew, title, "_"+strings.ToLower(title))
		}
		for i := 'A'; i <= 'Z'; i++ {
			oldnew = append(oldnew, string(i), "_"+string(i+32))
		}
		fullTitlesReplacer = strings.NewReplacer(oldnew...)
	}
	{
		structNameMap = make(map[string]string, len(fullTitles))
		for _, title := range fullTitles {
			structNameMap[strings.ToLower(title)] = title
		}
	}
}

// ToDBName 将结构体的字段名字转换成对应数据库字段名
func ToDBName(name string) string {
	val, ok := dbNameMap.Get(name)
	if ok {
		return val
	}
	return toDBName(name)
}

// ToStructName 数据库字段名转换成对应结构体名
func ToStructName(name string) string {
	if name == "" {
		return ""
	}

	val, ok := dbNameMap.Get(name)
	if ok {
		return val
	}

	return toStructName(name)
}

func toStructName(name string) string {
	sp := strings.Split(name, "_")
	for i := 0; i < len(sp); i++ {
		val := structNameMap[sp[i]]
		if val == "" {
			if len(sp[i]) > 0 && sp[i][0] >= 'a' && sp[i][0] <= 'z' {
				val = string(sp[i][0]-32) + sp[i][1:]
			}
		}
		sp[i] = val
	}
	structName := strings.Join(sp, "")
	dbNameMap.Set(name, structName)
	return structName
}

func toDBName(name string) string {
	dbName := fullTitlesReplacer.Replace(name)
	if len(dbName) >= 1 {
		dbNameMap.Set(name, dbName[1:])
		dbNameMap.Set(dbName[1:], name)
		return dbName[1:]
	}
	return ""
}

func ksvs(m map[string]interface{}, keyTail ...string) ([]string, []interface{}) {
	kt := ""
	ks := []string{}
	vs := []interface{}{}
	if len(keyTail) > 0 {
		kt = keyTail[0]
	}
	for k, v := range m {
		ks = append(ks, " `"+k+"`"+kt)
		vs = append(vs, v)
	}
	return ks, vs
}

// 用于返回对应个数参数,多用于In。
func argslice(l int) string {
	s := []string{}
	for i := 0; i < l; i++ {
		s = append(s, "?")
	}
	return strings.Join(s, ",")
}

// structToMap 将结构体转换成map[string]interface{}
func structToMap(v reflect.Value) map[string]interface{} {
	v = reflect.Indirect(v)
	t := v.Type()
	m := map[string]interface{}{}

	for i, num := 0, v.NumField(); i < num; i++ {
		tag := t.Field(i).Tag
		if tag.Get("crud") != "ignore" && tag.Get("crud") != "-" {
			if tag.Get("dbname") != "" {
				m[tag.Get("dbname")] = v.Field(i).Interface()
			} else {
				m[ToDBName(t.Field(i).Name)] = v.Field(i).Interface()
			}
		}
	}

	return m
}

// Placeholder sql占位
// n == 1 return ?
// n == 2 return ?,?
func Placeholder(n int) string {
	return placeholder(n)
}

func placeholder(n int) string {
	if n <= 10 {
		return placeholders[n]
	}
	holder := []string{}
	for i := 0; i < n; i++ {
		holder = append(holder, "?")
	}
	return strings.Join(holder, ",")
}

// MapsToCRUDRows convert []map[string]string to crud.RowsMap
func MapsToCRUDRows(m []map[string]string) RowsMap {
	rm := RowsMap{}
	for _, v := range m {
		rm = append(rm, RowMap(v))
	}
	return rm
}

func stringify(v interface{}) string {
	bs, _ := json.Marshal(&v)
	return string(bs)
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	newm := make(map[string]interface{}, len(m))
	for k, v := range m {
		newm[k] = v
	}
	return newm
}

// WhereTimeParse 将时间段转换成对应SQL
func WhereTimeParse(field, ts string, years, months, days int) string {
	// (createdtime >= '2018-01-01 00:00:00' AND createdtime < '2018-01-02 00:00:00')
	var a, b, format string
	format = "2006-01-02 15:04:05"[:len(ts)]
	t, _ := time.ParseInLocation(format, ts, time.Local)
	a = t.Format("2006-01-02 15:04:05")
	b = t.AddDate(years, months, days).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("(%s >= '%s' AND %s < '%s')", field, a, field, b)
}

func byteString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func stringByte(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// String 将传入的值转换成字符串
func String(v interface{}) string {
	var s string
	switch v := v.(type) {
	case int:
		s = strconv.Itoa(v)
	case int64:
		s = strconv.Itoa(int(v))
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

// Int 将传入的值转换成int
func Int(v interface{}) int {
	var i int
	switch v := v.(type) {
	case string:
		i, _ = strconv.Atoi(v)
	// 一个case多个值，就无法确认是什么类型了，就成了interface{}，所以要分开写。
	case int64:
		i = int(v)
	// 不实现除了uint64之后的无符号
	case uint64:
		i = int(v)
	case int:
		i = v
	case int8:
		i = int(v)
	case int16:
		i = int(v)
	case int32:
		i = int(v)
	default:
		i, _ = strconv.Atoi(fmt.Sprintf("%v", v))
	}
	return i
}
