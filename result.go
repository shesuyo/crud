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

// Pluge  获取某一列的interface类型
func (r *SQLRows) Pluge(cn string) []interface{} {
	if r.err != nil {
		return []interface{}{}
	}
	out := []interface{}{}
	rs := r.RawsMapInterface()
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
	rs := r.RawsMapInterface()
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
	rs := r.RawsMapInterface()
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

// RawMapInterface 返回map[string]interface{} 只有一列
func (r *SQLRows) RawMapInterface() map[string]interface{} {
	raws := r.RawsMapInterface()
	if len(raws) >= 1 {
		return raws[0]
	}
	return make(map[string]interface{}, 0)
}

// RawsMapInterface 返回[]map[string]interface{}，每个数组对应一列。
/*
	如果是无符号的tinyint能存0-255
	这里有浪费tinyint->int8[-128,127] unsigned tinyint uint8[0,255]，这里直接用int16[-32768,32767]
*/
func (r *SQLRows) RawsMapInterface() []map[string]interface{} {
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

// RawsMap []map[string]string 所有类型都将返回字符串类型
func (r *SQLRows) RawsMap() []map[string]string {
	rs := []map[string]string{}
	//panic: runtime error: invalid memory address or nil pointer dereference
	if r.err != nil {
		return rs
	}
	if r.rows == nil {
		return rs
	}
	cols, _ := r.rows.Columns()

	for r.rows.Next() {
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

// DoubleSlice 用于追求效率和更低的内存，但是使用比较不方便。
func (r *SQLRows) DoubleSlice() ([]string, [][]string) {
	cols := make([]string, 0)
	datas := make([][]string, 0)
	if r.err != nil {
		return cols, datas
	}
	cols, err := r.rows.Columns()
	if err != nil {
		return cols, datas
	}
	rawResult := make([][]byte, len(cols))
	dest := make([]interface{}, len(cols))
	for idx := range rawResult {
		dest[idx] = &rawResult[idx]
	}
	for r.rows.Next() {
		err := r.rows.Scan(dest...)
		if err != nil {
			return cols, datas
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
	return cols, datas
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
func (r *SQLRows) Find(v interface{}) {
	m := r.RawsMapInterface()
	rv := reflect.ValueOf(v).Elem()
	if rv.Kind() == reflect.Slice {
		for idx := range m {
			ele := reflect.New(rv.Type().Elem())
			for i := 0; i < ele.Elem().NumField(); i++ {
				dbn := ToDBName(ele.Elem().Type().Field(i).Name)
				dbv, ok := m[idx][dbn]
				if ok && dbv != nil {
					r.setValue(ele.Elem().Field(i), dbv)
				}
			}
			rv.Set(reflect.Append(rv, ele.Elem()))
		}

	} else {
		if len(m) == 1 {
			switch rv.Kind() {
			case reflect.Struct:
				elem := reflect.ValueOf(v).Elem()
				for i := 0; i < elem.NumField(); i++ {
					dbn := ToDBName(reflect.TypeOf(v).Elem().Field(i).Name)
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
		return r.rows.Scan(v)
	}
	return nil
}

func queryRows(rows *sql.Rows) []map[string]string {
	var result = make([]map[string]string, 0)
	for rows.Next() {
		result = append(result, scanRows(rows))
	}
	return result
}

func scanRows(rows *sql.Rows) map[string]string {
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
