package crud

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Table 是对CRUD进一层的封装
type Table struct {
	*DataBase
	*Search
	tableName string
}

// Name 返回名称
func (t *Table) Name() string {
	return t.tableName
}

// All 返回这张表所有数据
func (t *Table) All() RowsMap {
	return t.Query("SELECT * FROM " + t.tableName).RowsMap()
}

// // Count 返回表有多少条数据
// func (t *Table) Count() int {
// 	return t.Query("SELECT COUNT(*) FROM " + t.tableName).Int()

// }

// UpdateTime 查找表的更新时间
func (t *Table) UpdateTime() string {
	return t.Query("SELECT `UPDATE_TIME` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").String()

}

// AutoIncrement 查找表的自增ID的值
func (t *Table) AutoIncrement() int {
	return t.Query("SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").Int()
}

// SetAutoIncrement 设置自动增长ID
func (t *Table) SetAutoIncrement(id int) error {
	_, err := t.Exec("ALTER TABLE `" + t.tableName + "` AUTO_INCREMENT = " + strconv.Itoa(id)).RowsAffected()
	return err
}

// MaxID 查找表的最大ID，如果为NULL的话则为0
func (t *Table) MaxID() int {
	return t.Query("SELECT IFNULL(MAX(id), 0) as id FROM `" + t.tableName + "`").Int()

}

// IDIn 查找多个ID对应的列
func (t *Table) IDIn(ids ...interface{}) *SQLRows {
	if len(ids) == 0 {
		return &SQLRows{}
	}
	return t.Query(fmt.Sprintf("SELECT * FROM %s WHERE id in (%s)", t.tableName, argslice(len(ids))), ids...)
}

// Create 创建
// check 如果有，则会判断表里面以这几个字段为唯一的话，数据库是否存在此条数据，如果有就不插入了。
// 所有ORM的底层。FormXXX， (*DataBase)CRUD
//
func (t *Table) Create(m map[string]interface{}, checks ...string) (int64, error) {
	//INSERT INTO `feedback` (`task_id`, `template_question_id`, `question_options_id`, `suggestion`, `member_id`) VALUES ('1', '1', '1', '1', '1')
	if len(checks) > 0 {
		names := []string{}
		values := []interface{}{}
		for _, check := range checks {
			names = append(names, "`"+check+"`"+" = ? ")
			values = append(values, m[check])
		}
		// SELECT COUNT(*) FROM `feedback` WHERE `task_id` = ? AND `member_id` = ?
		if t.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE %s", t.tableName, strings.Join(names, "AND ")), values...).Int() > 0 {
			return 0, ErrInsertRepeat
		}
	}
	if t.tableColumns[t.tableName].HaveColumn(CreatedAt) {
		m[CreatedAt] = time.Now().Format(TimeFormat)
	}
	ks, vs := ksvs(m)
	id, err := t.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", t.tableName, strings.Join(ks, ","), argslice(len(ks))), vs...).LastInsertId()
	if err != nil {
		return 0, errors.New("SQL语句异常")
	}
	if id <= 0 {
		return 0, errors.New("插入数据库异常")
	}
	return id, nil
}

// CreateBatch create batch
// func (t *Table) CreateBatch(ms []map[string]interface{}) (int, error) {

// }

//Reads 查找
func (t *Table) Reads(m map[string]interface{}) RowsMap {
	if t.tableColumns[t.tableName].HaveColumn(IsDeleted) {
		m[IsDeleted] = 0
	}
	//SELECT * FROM address WHERE id = 1 AND uid = 27
	ks, vs := ksvs(m, " = ? ")
	return t.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RowsMap()
}

func (t *Table) Read(m map[string]interface{}) map[string]string {
	rs := t.Reads(m)
	if len(rs) > 0 {
		return rs[0]
	}
	return map[string]string{}
}

// Update 更新
// 如果map里面有id的话会自动删除id，然后使用id来作为更新的条件。
func (t *Table) Update(m map[string]interface{}, keys ...string) error {
	if len(keys) == 0 {
		keys = append(keys, "id")
	}
	if t.tableColumns[t.tableName].HaveColumn(UpdatedAt) {
		m[UpdatedAt] = time.Now().Format(TimeFormat)
	}
	keysValue := []interface{}{}
	whereks := []string{}
	for _, key := range keys {
		val, ok := m[key]
		if !ok {
			return errors.New("没有更新主键")
		}
		keysValue = append(keysValue, val)
		delete(m, key)
		whereks = append(whereks, "`"+key+"` = ? ")
	}
	//因为在更新的时候最好不要更新ID，而有时候又会将ID传入进来，所以id每次都会被删除，如果要更新id的话使用Exec()
	delete(m, "id")
	ks, vs := ksvs(m, " = ? ")
	for _, val := range keysValue {
		vs = append(vs, val)
	}
	_, err := t.Exec(fmt.Sprintf("UPDATE `%s` SET %s WHERE %s LIMIT 1", t.tableName, strings.Join(ks, ","), strings.Join(whereks, "AND")), vs...).RowsAffected()
	if err != nil {
		return errors.New("SQL语句异常")
	}
	return nil
}

//CreateOrUpdate 创建或者更新
func (t *Table) CreateOrUpdate(m map[string]interface{}, keys ...string) error {
	_, err := t.Create(m, keys...)
	if err != nil {
		if err == ErrInsertRepeat {
			return t.Update(m, keys...)
		}
		return err
	}
	return nil
}

// Delete 删除
func (t *Table) Delete(m map[string]interface{}) (int64, error) {
	ks, vs := ksvs(m, " = ? ")
	if t.tableColumns[t.tableName].HaveColumn(IsDeleted) {
		return t.Exec(fmt.Sprintf("UPDATE `%s` SET is_deleted = '1', deleted_at = '%s' WHERE %s", t.tableName, time.Now().Format(TimeFormat), strings.Join(ks, "AND")), vs...).RowsAffected()
	}
	return t.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RowsAffected()
}

// Clone 克隆
// 克隆要保证状态在每个链式操作后都是独立的。
func (t *Table) Clone() *Table {
	newTable := &Table{
		DataBase:  t.DataBase,
		tableName: t.tableName,
	}
	if t.Search == nil {
		newTable.Search = &Search{table: newTable, tableName: t.tableName}
	} else {
		newTable.Search = t.Search.Clone()
		newTable.Search.table = newTable
	}
	return newTable
}

//Where where
func (t *Table) Where(query string, args ...interface{}) *Table {
	return t.Clone().Search.Where(query, args...).table
}

//WhereNotEmpty 不允许空的
func (t *Table) WhereNotEmpty(query, arg string) *Table {
	if arg == "" {
		return t
	}
	return t.Clone().Search.Where(query, arg).table
}

//WhereStartEndDay 2017-07-07
func (t *Table) WhereStartEndDay(field, startDay, endDay string) *Table {
	if startDay == "" && endDay == "" {
		return t
	}
	if startDay != "" && endDay == "" {
		endDay = startDay
	}
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m-%d') >= ? AND DATE_FORMAT("+field+",'%Y-%m-%d') <= ?", startDay, endDay).table
}

//WhereStartEndMonth 2007-01
func (t *Table) WhereStartEndMonth(field, startMonth, endMonth string) *Table {
	if startMonth == "" && endMonth == "" {
		return t
	}
	if startMonth != "" && endMonth == "" {
		endMonth = startMonth
	}
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m') >= ? AND DATE_FORMAT("+field+",'%Y-%m') <= ?", startMonth, endMonth).table
}

//WhereStartEndTime 00:00
func (t *Table) WhereStartEndTime(field, startTime, endTime string) *Table {
	if startTime == "" && endTime == "" {
		return t
	}
	if startTime != "" && endTime == "" {
		endTime = startTime
	}
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%H:%i') >= ? AND DATE_FORMAT("+field+",'%H:%i') <= ?", startTime, endTime).table
}

//WhereToday 查看今天的。
func (t *Table) WhereToday(field string) *Table {
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m-%d') = ?", time.Now().Format("2006-01-02")).table
}

//WhereDay 这个字段等于这个时间
func (t *Table) WhereDay(field, time string) *Table {
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m-%d') = ?", time).table
}

//WhereMonth 这个字段等于这个时间
func (t *Table) WhereMonth(field, time string) *Table {
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m') = ?", time).table
}

//WhereBeforeToday 小于今天的时间
func (t *Table) WhereBeforeToday(field string) *Table {
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m-%d') < ?", time.Now().Format("2006-01-02")).table
}

//WhereLike field LIKE %like%
func (t *Table) WhereLike(field, like string) *Table {
	if like == "" {
		return t
	}
	return t.Clone().Search.Where(field+" LIKE ?", "%"+like+"%").table
}

//WhereLikeLeft field LIKE %like
func (t *Table) WhereLikeLeft(field, like string) *Table {
	return t.Clone().Search.Where(field+" LIKE ?", "%"+like).table
}

//WhereID id = ?
func (t *Table) WhereID(id interface{}) *Table {
	return t.Clone().Search.WhereID(id).table
}

//In In
func (t *Table) In(field string, args ...interface{}) *Table {
	return t.Clone().Search.In(field, args...).table
}

//Joins joins
func (t *Table) Joins(query string, args ...string) *Table {
	return t.Clone().Search.Joins(query, args...).table
}

//OrderBy OrderBy
func (t *Table) OrderBy(field string, isDESC ...bool) *Table {
	return t.Clone().Search.OrderBy(field, isDESC...).table
}

//Limit limit
func (t *Table) Limit(n interface{}) *Table {
	return t.Clone().Search.Limit(n).table
}

//Fields fields
func (t *Table) Fields(args ...string) *Table {
	return t.Clone().Search.Fields(args...).table
}
