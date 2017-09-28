package crud

import (
	"database/sql"
	"reflect"
	"strconv"
	"unsafe"
)

// SQLRows 查询后的返回结果
/*
	对于数据库底层的封装处理的时候一定要判断err是否为nil，因为当err为nil的时候sql.Rows也是nil，这样操作的时候就很容易出现错误了。
	if r.err != nil {
		return r.err
	}

	如果不使用连接池，出现Too many connections错误在并发量很大的时候很频繁。
*/
type SQLRows struct {
	rows *sql.Rows
	err  error
}

//为了兼容以前的代码这里设置四个转发的函数，以后肯定会慢慢移除掉的。

//RawMapInterface RowMapInterface
func (r *SQLRows) RawMapInterface() RowMapInterface {
	return r.RowMapInterface()
}

//RawsMapInterface RowsMapInterface
func (r *SQLRows) RawsMapInterface() RowsMapInterface {
	return r.RowsMapInterface()
}

//RawsMap RowsMap
func (r *SQLRows) RawsMap() RowsMap {
	return r.RowsMap()
}

//RawMap RowMap
func (r *SQLRows) RawMap() RowMap {
	return r.RowMap()
}

// Pluge  获取某一列的interface类型
func (r *SQLRows) Pluge(cn string) []interface{} {
	if r.err != nil {
		return []interface{}{}
	}
	out := []interface{}{}
	rs := r.RowsMapInterface()
	for _, v := range rs {
		out = append(out, v[cn])
	}
	return out
}

// PlugeInt 获取某一列的int类型
func (r *SQLRows) PlugeInt(cn string) []int {
	if r.err != nil {
		return []int{}
	}
	out := []int{}
	rs := r.RowsMapInterface()
	for _, v := range rs {
		i, ok := v[cn].(int)
		if ok {
			out = append(out, i)
		} else {
			return []int{}
		}
	}
	return out
}

// PlugeStinrg 获取某一列的string类型
func (r *SQLRows) PlugeStinrg(cn string) []string {
	if r.err != nil {
		return []string{}
	}
	out := []string{}
	rs := r.RowsMapInterface()
	for _, v := range rs {
		i, ok := v[cn].(string)
		if ok {
			out = append(out, i)
		} else {
			return []string{}
		}
	}
	return out
}

// RowMapInterface 返回map[string]interface{} 只有一列
func (r *SQLRows) RowMapInterface() RowMapInterface {
	raws := r.RowsMapInterface()
	if len(raws) >= 1 {
		return raws[0]
	}
	return make(map[string]interface{}, 0)
}

// RowsMapInterface 返回[]map[string]interface{}，每个数组对应一列。
/*
	如果是无符号的tinyint能存0-255
	这里有浪费tinyint->int8[-128,127] unsigned tinyint uint8[0,255]，这里直接用int16[-32768,32767]
*/
func (r *SQLRows) RowsMapInterface() RowsMapInterface {
	rs := []map[string]interface{}{}
	if r.err != nil {
		return rs
	}
	cols, err := r.rows.Columns()
	if err != nil {
		return rs
	}

	for r.rows.Next() {

		// 数据库查询的一列
		rowMap := make(map[string]interface{})

		/*
			用于放到底层去取数据的容器
			type RawBytes []byte
		*/
		var b sql.RawBytes = []byte("abc")
		_ = string(b)
		containers := make([]interface{}, 0, len(cols))
		for i := 0; i < cap(containers); i++ {
			col := DBColums[cols[i]]
			switch col.DataType {
			case "int", "tinyint", "smallint", "mediumint", "integer":
				var v int
				containers = append(containers, &v)
			case "varchar", "bigint", "timestamp":
				var v string
				containers = append(containers, &v)
			case "bit":
				fallthrough
			default:
				containers = append(containers, &sql.RawBytes{})
			}

		}
		r.rows.Scan(containers...)
		for i := 0; i < len(cols); i++ {
			//rowMap[cols[i]] = string(*containers[i].(*[]byte))
			for i := 0; i < cap(containers); i++ {
				col := DBColums[cols[i]]
				switch col.DataType {
				case "int", "tinyint", "smallint", "mediumint", "integer", "varchar", "bigint", "timestamp":
					rowMap[cols[i]] = reflect.ValueOf(containers[i]).Elem().Interface()
				default:
					rowMap[cols[i]] = string(*containers[i].(*sql.RawBytes))
				}
			}
		}
		rs = append(rs, rowMap)
	}
	return rs
}

type (
	//RowsMap 多行
	RowsMap []map[string]string
	//RowMap 单行
	RowMap map[string]string
	//RowsMapInterface 多行
	RowsMapInterface []map[string]interface{}
	//RowMapInterface 单行
	RowMapInterface map[string]interface{}
)

//HaveID 是否有这个ID
func (rm RowsMap) HaveID(id string) bool {
	for _, v := range rm {
		if v["id"] == id {
			return true
		}
	}
	return false
}

//Pluck 取出中间的一列
func (rm RowsMap) Pluck(key string) []interface{} {
	var vs []interface{}
	for _, v := range rm {
		vs = append(vs, v[key])
	}
	return vs
}

//PluckString 取出中间的一列
func (rm RowsMap) PluckString(key string) []string {
	var vs []string
	for _, v := range rm {
		vs = append(vs, v[key])
	}
	return vs
}

// RowsMap []map[string]string 所有类型都将返回字符串类型
func (r *SQLRows) RowsMap() RowsMap {
	rs := make([]map[string]string, 0) //为了JSON输出的时候为[]
	//rs := []map[string]string{} //这样在JSON输出的时候是null

	//panic: runtime error: invalid memory address or nil pointer dereference
	if r.err != nil {
		return rs
	}
	if r.rows == nil {
		return rs
	}
	cols, _ := r.rows.Columns()

	for r.rows.Next() {
		//type RawBytes []byte
		rowMap := make(map[string]string)
		containers := make([]interface{}, 0, len(cols))
		for i := 0; i < cap(containers); i++ {
			containers = append(containers, &[]byte{})
		}
		r.rows.Scan(containers...)
		for i := 0; i < len(cols); i++ {
			rowMap[cols[i]] = string(*containers[i].(*[]byte))
			//rowMap[cols[i]] = *(*string)(unsafe.Pointer(containers[i].(*[]byte)))
		}
		rs = append(rs, rowMap)
	}
	return rs
}

//RowMap RowMap
func (r *SQLRows) RowMap() RowMap {
	out := r.RowsMap()
	if len(out) > 0 {
		return out[0]
	}
	return map[string]string{}
}

// DoubleSlice 用于追求效率和更低的内存，但是使用比较不方便。
func (r *SQLRows) DoubleSlice() (map[string]int, [][]string) {
	cols := make([]string, 0)
	datas := make([][]string, 0)
	if r.err != nil {
		return map[string]int{}, datas
	}
	cols, err := r.rows.Columns()
	if err != nil {
		return map[string]int{}, datas
	}
	rawResult := make([][]byte, len(cols))
	dest := make([]interface{}, len(cols))
	for idx := range rawResult {
		dest[idx] = &rawResult[idx]
	}
	for r.rows.Next() {
		err := r.rows.Scan(dest...)
		if err != nil {
			return map[string]int{}, datas
		}
		result := make([]string, len(cols))
		for i, raw := range rawResult {
			if raw == nil {
				result[i] = ""
			} else {
				result[i] = *(*string)(unsafe.Pointer(&raw))
			}
		}
		datas = append(datas, result)
	}
	m := make(map[string]int, len(cols))
	for k, v := range cols {
		m[v] = k
	}
	return m, datas
}

// Int SCAN 一个int类型，只能在只有一列中使用。
func (r *SQLRows) Int() int {
	if r.err != nil {
		return 0
	}
	count := 0
	r.Scan(&count)
	return count
}

func (r *SQLRows) String() string {
	if r.err != nil {
		return ""
	}
	str := ""
	r.Scan(&str)
	return str
}

// Find 将结果查找后放到结构体中
func (r *SQLRows) Find(v interface{}) error {
	m := r.RowsMapInterface()
	rv := reflect.ValueOf(v).Elem()
	//如果查询是数组的话
	if rv.Kind() == reflect.Slice {
		for idx := range m {
			elem := reflect.New(rv.Type().Elem())
			for i := 0; i < elem.Elem().NumField(); i++ {
				var dbn string
				var field = elem.Elem().Type().Field(i)
				var tagName = field.Tag.Get("dbname")
				if tagName != "" {
					dbn = tagName
				} else {
					dbn = ToDBName(field.Name)
				}
				dbv, ok := m[idx][dbn]
				if ok && dbv != nil {
					r.setValue(elem.Elem().Field(i), dbv)
				}
			}
			rv.Set(reflect.Append(rv, elem.Elem()))
		}

	} else {
		//查询的是一个结构体或者是一个int,一个string
		//以后如果只是一个结果的话，支持ToInt/ToString/ToInterface
		//这里的int要去除掉
		if len(m) >= 1 {
			switch rv.Kind() {
			case reflect.Struct:
				elem := reflect.ValueOf(v).Elem()
				for i := 0; i < elem.NumField(); i++ {
					var dbn string
					var field = elem.Type().Field(i)
					var tagName = field.Tag.Get("dbname")
					if tagName != "" {
						dbn = tagName
					} else {
						dbn = ToDBName(field.Name)
					}
					dbv, ok := m[0][dbn]
					if ok && dbv != nil {
						r.setValue(elem.Field(i), dbv)
					}
				}
			case reflect.Int, reflect.Int64:
				for _, v := range m[0] {
					str, ok := v.(string)
					if ok {
						val, err := strconv.Atoi(str)
						if err == nil {
							r.setValue(rv, val)
						}
					}
				}
			default:
				for _, v := range m[0] {
					r.setValue(rv, v)
				}
			}
		}
	}
	return nil
}

func (r *SQLRows) setValue(v reflect.Value, i interface{}) {
	if i != nil && v.Interface() != nil {
		v.Set(reflect.ValueOf(i))
	}
}

// Scan 当只需要一列中的一个数据是可以使用Scan,比如 select count(*) from tablename
func (r *SQLRows) Scan(v interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.rows.Next() {
		err := r.rows.Scan(v)
		r.rows.Close()
		return err
	}
	return nil
}

func queryRows(rows *sql.Rows) RowsMap {
	var result = make([]map[string]string, 0)
	for rows.Next() {
		result = append(result, scanRows(rows))
	}
	return result
}

func scanRows(rows *sql.Rows) RowMap {
	var result = make(map[string]string)

	cols, _ := rows.Columns()

	containers := make([]interface{}, 0, len(cols))
	for i := 0; i < cap(containers); i++ {
		var v sql.RawBytes
		containers = append(containers, &v)
	}

	err := rows.Scan(containers...)
	if err != nil {
		return nil
	}

	for i, v := range containers {
		value := reflect.Indirect(reflect.ValueOf(v)).Bytes()
		result[cols[i]] = string(value)
	}

	return result
}

// SQLResult 是一个封装了sql.Result 的结构体
//type SQLResult struct {
//	ret sql.Result
//	err error
//}

//// ID 获取插入的ID
//func (r *SQLResult) ID() (int64, error) {
//	if r.err != nil {
//		return 0, r.err
//	}
//	id, err := r.ret.LastInsertId()
//	if err != nil {
//		return 0, err
//	}
//	return id, nil
//}

//// Effected 获取影响行数
//func (r *SQLResult) Effected() (int64, error) {
//	if r.err != nil {
//		return 0, r.err
//	}
//	affected, err := r.ret.RowsAffected()
//	if err != nil {
//		return 0, err
//	}
//	return affected, nil
//}
