package crud

import (
	"database/sql"
	"reflect"
	"strconv"
)

type SQLRows struct {
	rows *sql.Rows
	err  error
}

func (r *SQLRows) RawMapInterface() map[string]interface{} {
	raws := r.RawsMapInterface()
	if len(raws) >= 1 {
		return raws[0]
	}
	return make(map[string]interface{}, 0)
}

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

func (r *SQLRows) RawsMap() []map[string]string {
	rs := []map[string]string{}
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
		}
		rs = append(rs, rowMap)
	}
	return rs
}

func (r *SQLRows) Find(v interface{}) {
	m := r.RawsMapInterface()
	rv := reflect.ValueOf(v).Elem()
	if rv.Kind() == reflect.Slice {
		for idx, _ := range m {
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

func (r *SQLRows) Scan(v interface{}) error {
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

type SQLResult struct {
	ret sql.Result
	err error
}

func (r *SQLResult) ID() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	id, err := r.ret.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *SQLResult) Effected() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	affected, err := r.ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}
