package crud

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
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

// RawMapInterface RowMapInterface
func (r *SQLRows) RawMapInterface() RowMapInterface {
	return r.RowMapInterface()
}

// RawsMapInterface RowsMapInterface
func (r *SQLRows) RawsMapInterface() RowsMapInterface {
	return r.RowsMapInterface()
}

// RawsMap RowsMap
func (r *SQLRows) RawsMap() RowsMap {
	return r.RowsMap()
}

// RawMap RowMap
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
			out = append(out, Int(v[cn]))
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
			out = append(out, String(v[cn]))
		}
	}
	return out
}

// RowMapInterface 返回map[string]interface{} 只有一列
func (r *SQLRows) RowMapInterface() RowMapInterface {
	rows := r.RowsMapInterface()
	if len(rows) >= 1 {
		return rows[0]
	}
	return make(map[string]interface{}, 0)
}

// RowsMapInterface 返回[]map[string]interface{}，每个数组对应一列。
/*
	如果是无符号的tinyint能存0-255
	这里有浪费tinyint->int8[-128,127] unsigned tinyint uint8[0,255]，这里直接用int16[-32768,32767]
*/
func (r *SQLRows) RowsMapInterface() RowsMapInterface {
	// _st := time.Now()
	// defer func() {
	// 	fmt.Println(time.Now().Sub(_st))
	// }()
	rs := RowsMapInterface{}
	if r.err != nil {
		return rs
	}
	// ct, err := r.rows.ColumnTypes()
	// fmt.Println(err)
	// for _, v := range ct {
	// 	fmt.Println(wtf.JSONStringify(v))
	// }

	// fmt.Println(wtf.JSONStringify(r.rows))
	// fmt.Println(r.rows)

	// https://segmentfault.com/a/1190000003036452

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
		// var b sql.RawBytes = []byte("abc")
		// _ = string(b)
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
	RowsMap []RowMap
	//RowMap 单行
	RowMap map[string]string
	//RowsMapInterface 多行
	RowsMapInterface []RowMapInterface
	//RowMapInterface 单行
	RowMapInterface map[string]interface{}
)

// MarshalJSON 实现MarshalJSON
// func (rm RowMap) MarshalJSON() ([]byte, error) {
// 	sb := bytes.NewBuffer(make([]byte, 0, 1024))
// 	sb.WriteByte('{')
// 	l := len(rm)
// 	n := 0
// 	for k, v := range rm {
// 		sb.WriteByte('"')
// 		sb.Write(stringByte(k))
// 		sb.Write([]byte{'"', ':', '"'})
// 		if strings.Contains(v, "\\") {
// 			v = strings.Replace(v, "\\", "\\\\", -1)
// 		}
// 		if strings.Contains(v, `"`) {
// 			sb.Write([]byte(strings.Replace(v, `"`, `\"`, -1)))
// 		} else {
// 			sb.Write([]byte(v))
// 		}
// 		sb.WriteByte('"')
// 		n++
// 		if n < l {
// 			sb.WriteByte(',')
// 		}
// 	}
// 	sb.WriteByte('}')
// 	return sb.Bytes(), nil
// }

// Bool return singel bool
func (rm RowMap) Bool(field ...string) bool {
	if rm.NotFound() {
		return false
	}
	if len(field) > 0 {
		if rm[field[0]] == "1" {
			return true
		}
		return false
	}
	for _, v := range rm {
		if v == "1" {
			return true
		}
		return false
	}
	return false
}

// FieldDefault get field if not reture the def value
func (rm RowMap) FieldDefault(field, def string) string {
	val, ok := rm[field]
	if !ok {
		val = def
	}
	return val
}

// Interface conver RowMap to RowMapInterface
func (rm RowMap) Interface() RowMapInterface {
	rmi := RowMapInterface{}
	for k, v := range rm {
		rmi[k] = v
	}
	return rmi
}

// HaveRecord return it's have record
func (rm RowMap) HaveRecord() bool {
	if len(rm) > 0 {
		return true
	}
	return false
}

// NotFound return it's not found
func (rm RowMap) NotFound() bool {
	if len(rm) == 0 {
		return true
	}
	return false
}

// Int return int field
func (rm RowMap) Int(field string, def ...int) int {
	val, ok := rm[field]
	if ok {
		i, err := strconv.Atoi(val)
		if err == nil {
			return i
		}
	}
	if len(def) > 0 {
		return def[0]
	}
	return 0
}

// Float64 return float64
func (rm RowMap) Float64(field string, def ...float64) float64 {
	val, ok := rm[field]
	if ok {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return f
		}
	}
	if len(def) > 0 {
		return def[0]
	}
	return 0
}

// Copy 复制一份
func (rm RowsMap) Copy() RowsMap {
	nrm := make(RowsMap, len(rm))
	for _, r := range rm {
		nr := RowMap{}
		for k, v := range r {
			nr[k] = v
		}
		nrm = append(nrm, nr)
	}
	return nrm
}

// Sum Sum
func (rm RowsMap) Sum(field string) int {
	sum := 0
	for _, v := range rm {
		sum += v.Int(field)
	}
	return sum
}

// String return map[string]string
func (rm RowsMap) String() []map[string]string {
	ms := []map[string]string{}
	for _, r := range rm {
		ms = append(ms, map[string]string(r))
	}
	return ms
}

// Interface conver RowsMap to RowsMapInterface
func (rm RowsMap) Interface() RowsMapInterface {
	rmi := RowsMapInterface{}
	for _, v := range rm {
		rmi = append(rmi, v.Interface())
	}
	return rmi
}

// MapIndex 按照指定field划分成map[string]RowMap
func (rm RowsMap) MapIndex(field string) map[string]RowMap {
	sr := make(map[string]RowMap, len(rm))
	for _, r := range rm {
		sr[r[field]] = r
	}
	return sr
}

// MapIndexKV 按照key，val 转换成 map[string]string
func (rm RowsMap) MapIndexKV(key, val string) map[string]string {
	ss := make(map[string]string, 0)
	for _, r := range rm {
		ss[r[key]] = r[val]
	}
	return ss
}

// MapIndexs 按照指定field划分成map[string]RowsMap
func (rm RowsMap) MapIndexs(field string) map[string]RowsMap {
	sr := make(map[string]RowsMap, len(rm))
	for _, r := range rm {
		sr[r[field]] = append(sr[r[field]], r)
	}
	return sr
}

// Vals vals
type Vals []string

// Contains vals weather contains s
func (vs Vals) Contains(s string) bool {
	for _, v := range vs {
		if v == s {
			return true
		}
	}
	return false
}

// MapIndexsKV 按k,v划分成map[string]Vals
func (rm RowsMap) MapIndexsKV(key, val string) map[string]Vals {
	sv := make(map[string]Vals)
	for _, r := range rm {
		sv[r[key]] = append(sv[r[key]], r[val])
	}
	return sv
}

// Filter 过滤指定字段
func (rm RowsMap) Filter(field, equal string) RowsMap {
	frm := RowsMap{}
	for _, v := range rm {
		if v[field] == equal {
			frm = append(frm, v)
		}
	}
	return frm
}

// FilterIn 指定字段在数组里面皆会被挑选出来
func (rm RowsMap) FilterIn(field string, equals []string) RowsMap {
	em := make(map[string]bool, len(equals))
	for _, e := range equals {
		em[e] = true
	}
	frm := RowsMap{}
	for _, v := range rm {
		if em[v[field]] {
			frm = append(frm, v)
		}
	}
	return frm
}

// Len 返回RowsMap的长度
func (rm RowsMap) Len() int {
	return len(rm)
}

// First 返回RowsMap的第一个元素，如果没有则返回空RowMap
func (rm RowsMap) First() RowMap {
	if len(rm) > 0 {
		return rm[0]
	}
	return RowMap{}
}

// FilterNotIn 指定字段在数组里面皆不会被挑选出来
func (rm RowsMap) FilterNotIn(field string, equals []string) RowsMap {
	em := make(map[string]bool, len(equals))
	for _, e := range equals {
		em[e] = true
	}
	frm := RowsMap{}
	for _, v := range rm {
		if !em[v[field]] {
			frm = append(frm, v)
		}
	}
	return frm
}

// FilterFunc fileter by func (like jq)
// return true will be append
func (rm RowsMap) FilterFunc(equalF func(RowMap) bool) RowsMap {
	frm := RowsMap{}
	for _, v := range rm {
		if equalF(v) {
			frm = append(frm, v)
		}
	}
	return frm
}

// EachAddTableString 根据一个字段查找
// https://github.com/shesuyo/crud/issues/11
// 第一个是原来的，第二个是新的。
// rowmapfield tablefield tablefield rowmapfield
func (rm *RowsMap) EachAddTableString(table *Table, args ...string) {
	argsLen := len(args)
	if argsLen < 4 || argsLen%2 != 0 {
		return
	}
	fiels := []string{args[1]}
	for i := 2; i < argsLen; i += 2 {
		fiels = append(fiels, args[i])
	}
	datas := table.Fields(fiels...).In(args[1], rm.Pluck(args[0])...).RowsMap()
	rmLen := len(*rm)
	datasLen := len(datas)
	for i := 0; i < rmLen; i++ {
		isFound := false
		for j := 0; j < datasLen; j++ {
			if (*rm)[i][args[0]] == datas[j][args[1]] {
				isFound = true
				for k := 2; k < argsLen; k += 2 {
					(*rm)[i][args[k+1]] = datas[j][args[k]]
				}
				break
			}
		}
		if !isFound {
			for k := 2; k < argsLen; k += 2 {
				(*rm)[i][args[k+1]] = ""
			}
		}
	}
}

// EachMod mod each rowmap
func (rm RowsMap) EachMod(f func(RowMap)) RowsMap {
	l := len(rm)
	for i := 0; i < l; i++ {
		f(rm[i])
	}
	return rm
}

// GroupByField group by field
// func (rm RowsMap) GroupByField(field string) map[string][]RowMap {
// 	gm := map[string][]RowMap{}
// 	for _, v := range rm {
// 		gm[v[field]] = append(gm[v[field]], v)
// 	}
// 	return gm
// }

// RowsMapGroup 用于对一个字段进行分组
type RowsMapGroup struct {
	Key  string   `json:"key"`
	Len  int      `json:"len"`
	Vals []RowMap `json:"vals"`
}

// GroupByField 用field字段进行分组
func (rm RowsMap) GroupByField(field string) []RowsMapGroup {
	gm := map[string][]RowMap{}
	orders := []string{}
	for _, v := range rm {
		_, ok := gm[v[field]]
		if !ok {
			orders = append(orders, v[field])
		}
		gm[v[field]] = append(gm[v[field]], v)
	}
	rmg := make([]RowsMapGroup, 0, len(orders))
	for _, key := range orders {
		tmp := RowsMapGroup{
			Key:  key,
			Vals: gm[key],
		}
		tmp.Len = len(tmp.Vals)
		rmg = append(rmg, tmp)
	}
	return rmg
}

// RowsWrap WarpByField
type RowsWrap struct {
	Key string   `json:"key"`
	Val []RowMap `json:"val"`

	less func(i, j int) bool
}

// RowsWraps []RowsWrap
type RowsWraps []RowsWrap

// RowsWrapsSortFunc RowsWrapsSort sort func
type RowsWrapsSortFunc func(rm *RowsWraps, i, j int) bool

// RowsWrapsSort sort for RowsWraps
type RowsWrapsSort struct {
	rm *RowsWraps
	f  RowsWrapsSortFunc
}

// Len len
func (rs RowsWrapsSort) Len() int {
	return len(*(rs.rm))
}

// Swap swap
func (rs RowsWrapsSort) Swap(i, j int) {
	(*(rs.rm))[i], (*(rs.rm))[j] = (*(rs.rm))[j], (*(rs.rm))[i]
}

// Less Less
func (rs RowsWrapsSort) Less(i, j int) bool {
	return rs.f(rs.rm, i, j)
}

// Sort Sort
func (rw *RowsWraps) Sort(f RowsWrapsSortFunc) {
	rf := RowsWrapsSort{rm: rw, f: f}
	sort.Sort(rf)
}

// HaveKey return weather have this key
func (rw RowsWraps) HaveKey(key string) bool {
	for _, v := range rw {
		if v.Key == key {
			return true
		}
	}
	return false
}

// Set auto judge & set key
func (rw *RowsWraps) Set(key string, val RowMap) {
	if rw.HaveKey(key) {
		l := len(*rw)
		for i := 0; i < l; i++ {
			if (*rw)[i].Key == key {
				(*rw)[i].Val = append((*rw)[i].Val, val)
			}
		}
	} else {
		if val == nil {
			(*rw) = append((*rw), RowsWrap{Key: key, Val: make(RowsMap, 0, 2)})
		} else {
			(*rw) = append((*rw), RowsWrap{Key: key, Val: []RowMap{val}})
		}
	}
}

// WarpByField WarpByField
func (rm RowsMap) WarpByField(field string) RowsWraps {
	rw := RowsWraps{}
	for _, v := range rm {
		rw.Set(v[field], v)
	}
	return rw
}

// MultiWarp multi warp
type MultiWarp struct {
	ID     string      `json:"id"`
	Name   string      `json:"name"`
	Vals   []MultiWarp `json:"vals"`
	preID  string
	tailID string
}

// MultiWarpByField multi level warp by field
// fields id1,key1,id2,key2,id3,key3
// 先传id再传字段
// if RowMap[key] == "" , abandon key+n(n>=0)
// 方案1：一层一层的进行拼接
// 方案2： 所有层次一起拼接
// 方案3： 从最后一层向前拼接
// 方案4： 所有节点一起算出来，然后再进行首尾拼接。
// 同头同尾也会有问题
/*
3 2 1
3 2 2
4 2 3
*/
func (rm RowsMap) MultiWarpByField(fields ...string) []MultiWarp {

	length := len(fields)

	if length == 0 || length%2 == 1 {
		return []MultiWarp{}
	}

	fields = append(fields, "", "")
	fields = append([]string{"", ""}, fields...)

	levels := make([][]MultiWarp, length/2)

	for _, r := range rm {
		levelIdx := len(levels) - 1
		for i := len(fields) - 4; i >= 2; i -= 2 {
			// fmt.Println("i,levelIdx:", i, levelIdx)
			isAppend := false
			// 不需要ID为0的项，认为不存在。
			if r[fields[i]] == "0" || r[fields[i]] == "" {
				// 这里没有减层，所以之前有BUG
				// 以后需要注意在continue的时候没有处理for最后面的loop处理
				levelIdx--
				continue
			}
			ps := []string{}
			for j := 2; j < i; j += 2 {
				ps = append(ps, r[fields[j]])
			}
			var pre string //preID: r[fields[i-2]],
			pre = strings.Join(ps, "-")
			var tail string
			if pre == "" {
				tail = r[fields[i]]
			} else {
				tail = pre + "-" + r[fields[i]]
			}
			newWarp := MultiWarp{
				ID:     r[fields[i]],
				Name:   r[fields[i+1]],
				preID:  pre,
				tailID: tail,
				Vals:   make([]MultiWarp, 0),
			}

			// fmt.Println(levelIdx, pre, tail, fmt.Sprintf("i:%d", i), fields[i])
			// newWarp := MultiWarp{ID: r[fields[i]], Name: r[fields[i+1]], preID: r[fields[i-2]], tailID: r[fields[i+2]], Vals: make([]MultiWarp, 0)}
			// fmt.Println(r, stringify(newWarp))
			// newWarp := MultiWarp{ID: r[fields[i]], Name: r[fields[i+1]], preID: r[fields[i-2]], tailID: r[fields[i+2]]}
			for _, level := range levels[levelIdx] {
				if pre == tail {
					isAppend = true
					break
				}
				if level.ID == newWarp.ID && level.preID == newWarp.preID {
					// 只要父节点和本身节点是一样的话，就是重复的了。
					// 因为这棵树是从前面（idx=0）开始的，所以不能以同一个尾判断是同一个，同级下也可能有相同的尾。
					// if level.ID == newWarp.ID && level.preID == newWarp.preID && level.tailID == newWarp.tailID {
					isAppend = true
					break
				}
			}
			if !isAppend {
				levels[levelIdx] = append(levels[levelIdx], newWarp)
			}
			levelIdx--
		}
	}

	// for i := 0; i < len(levels); i++ {
	// 	fmt.Println(i, len(levels[i]), stringify(levels[i]))
	// }

	// 从倒数第二个level开始向前合并
	for i := len(levels) - 2; i >= 0; i-- {
		for lIdx := 0; lIdx < len(levels[i]); lIdx++ {
			for cIdx := 0; cIdx < len(levels[i+1]); cIdx++ {
				// 如果前面的尾巴是
				// tailID其实在这里是没有用的，因为不同的tailID,可能已经被除重了。
				if levels[i][lIdx].tailID == levels[i+1][cIdx].preID {
					levels[i][lIdx].Vals = append(levels[i][lIdx].Vals, levels[i+1][cIdx])
				} else {
					// not match
					// fmt.Println(levels[i][lIdx].Name, levels[i][lIdx].tailID, levels[i+1][cIdx].Name, levels[i+1][cIdx].ID)
				}
			}
		}
	}
	// jsonText := any.JSONStringify(levels[0])
	// fmt.Println("write", len(jsonText))
	// ioutil.WriteFile("a.json", []byte(jsonText), 0777)
	// fmt.Println("done")
	if levels[0] == nil {
		return []MultiWarp{}
	}
	return levels[0]
}

// HaveID 是否有这个ID
func (rm RowsMap) HaveID(id string) bool {
	for _, v := range rm {
		if v["id"] == id {
			return true
		}
	}
	return false
}

// RowID 根据id获取所在行
func (rm RowsMap) RowID(id string) RowMap {
	for _, v := range rm {
		if v["id"] == id {
			return v
		}
	}
	return nil
}

// RowField 根据字段获取所在行
func (rm RowsMap) RowField(val, field string) RowMap {
	for _, v := range rm {
		if v[field] == val {
			return v
		}
	}
	return RowMap{}
}

// RowsField 根据字段找出多列
func (rm RowsMap) RowsField(val, field string) RowsMap {
	rows := RowsMap{}
	for _, v := range rm {
		if v[field] == val {
			rows = append(rows, v)
		}
	}
	return rows
}

// Pluck 取出中间的一列
func (rm RowsMap) Pluck(key string) []interface{} {
	var vs = make([]interface{}, 0)
	for _, v := range rm {
		vs = append(vs, v[key])
	}
	return vs
}

// PluckString pluck field with string
func (rm RowsMap) PluckString(key string) []string {
	var vs = make([]string, 0)
	for _, v := range rm {
		vs = append(vs, v[key])
	}
	return vs
}

// PluckInt pluck field with int
func (rm RowsMap) PluckInt(key string) []int {
	var vs = make([]int, 0)
	for _, v := range rm {
		val, _ := strconv.Atoi(v[key])
		vs = append(vs, val)
	}
	return vs
}

// Unique unique field
func (rm RowsMap) Unique(field string) RowsMap {
	urm := RowsMap{}
	for i := 0; i < len(rm); i++ {
		isUnique := true
		for j := 0; j < len(urm); j++ {
			if urm[j][field] == rm[i][field] {
				isUnique = false
				break
			}
		}
		if isUnique {
			urm = append(urm, rm[i])
		}
	}
	return urm
}

// RowsMapSort struct for sort.Sort
type RowsMapSort struct {
	rm *RowsMap
	f  func(RowsMap, int, int) bool
}

// Len len
func (rs RowsMapSort) Len() int {
	return len(*(rs.rm))
}

// Swap swap
func (rs RowsMapSort) Swap(i, j int) {
	(*(rs.rm))[i], (*(rs.rm))[j] = (*(rs.rm))[j], (*(rs.rm))[i]
}

func (rs RowsMapSort) Less(i, j int) bool {
	return rs.f(*(rs.rm), i, j)
}

// Sort sort by string field
// default aes
func (rm *RowsMap) Sort(field string, isDesc bool) *RowsMap {
	return rm.SortFunc(func(rm RowsMap, i, j int) bool {
		if isDesc {
			return rm[i][field] > rm[j][field]
		}
		return rm[i][field] < rm[j][field]
	})
}

// SortInt sort by int field
func (rm *RowsMap) SortInt(field string, isDesc bool) *RowsMap {
	return rm.SortFunc(func(rm RowsMap, i, j int) bool {
		if isDesc {
			return rm[i].Int(field) > rm[j].Int(field)
		}
		return rm[i].Int(field) < rm[j].Int(field)
	})
}

// SortFunc sort by func
func (rm *RowsMap) SortFunc(f func(RowsMap, int, int) bool) *RowsMap {
	rms := RowsMapSort{rm: rm, f: f}
	sort.Sort(rms)
	return rm
}

// String get string field from RowMapInterface
func (rm RowMapInterface) String(field string) string {
	str, ok := rm[field].(string)
	if ok {
		return str
	}
	str = fmt.Sprintf("%v", rm[field])
	return str
}

// Bytes 返回对应值的[]byte
func (rm RowMapInterface) Bytes(field string) []byte {
	return []byte(rm.String(field))
}

// Int get int field from RowMapInterface
func (rm RowMapInterface) Int(field string) int {
	str, ok := rm[field].(string)
	if ok {
		i, _ := strconv.Atoi(str)
		return i
	}
	str = fmt.Sprintf("%v", rm[field])
	i, _ := strconv.Atoi(str)
	return i
}

// RowMap convert RowMapInterface to RowMap
func (rm RowMapInterface) RowMap() RowMap {
	r := RowMap{}
	for k, v := range rm {
		r[k] = fmt.Sprintf("%v", v)
	}
	return r
}

// RowsMap []map[string]string 所有类型都将返回字符串类型
func (r *SQLRows) RowsMap() RowsMap {
	rs := make([]RowMap, 0) //为了JSON输出的时候为[]
	//rs := []map[string]string{} //这样在JSON输出的时候是null

	//panic: runtime error: invalid memory address or nil pointer dereference
	if r.err != nil {
		return rs
	}
	if r.rows == nil {
		return rs
	}
	cols, _ := r.rows.Columns()
	// ts, _ := r.rows.ColumnTypes()
	// for _, t := range ts {
	// 	fmt.Printf("%#v\n", *t)
	// }

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

// RowMap RowMap
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
		switch v.Kind() {
		case reflect.Int:
			v.Set(reflect.ValueOf(Int(i)))
		case reflect.String:
			v.Set(reflect.ValueOf(String(i)))
		default:
			v.Set(reflect.ValueOf(i))
		}

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
	var result = make([]RowMap, 0)
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

// Explain sql explain struct
type Explain struct {
	ID           int    `json:"id"`
	SelectType   string `json:"select_type"`
	Table        string `json:"table"`
	Partitions   string `json:"partitions"`
	Type         string `json:"type"`
	PossibleKeys string `json:"possible_keys"`
	Key          string `json:"key"`
	KeyLen       int    `json:"key_len"`
	Ref          string `json:"ref"`
	Rows         int    `json:"rows"`
	Filtered     int    `json:"filtered"`
	Extra        string `json:"extra"`
}

func (e Explain) String() string {
	return fmt.Sprintf("%#v", e)
}
