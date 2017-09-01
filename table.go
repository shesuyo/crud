package crud

import "strconv"
import "fmt"
import "strings"
import "errors"

var (
	errInsertRepeat = errors.New("重复插入")
	errSQLSyncPanic = errors.New("SQL语句异常")
	errInsertData   = errors.New("插入数据库异常")
	errNoUpdateKey  = errors.New("没有更新主键")
)

// Table 是对CRUD进一层的封装
type Table struct {
	*CRUD
	tableName string
}

// Name 返回名称
func (t *Table) Name() string {
	return t.tableName
}

// All 返回这张表所有数据
func (t *Table) All() []map[string]string {
	return t.Query("SELECT * FROM " + t.tableName).RawsMap()
}

// Count 返回表有多少条数据
func (t *Table) Count() (count int) {
	t.Query("SELECT COUNT(*) FROM " + t.tableName).Scan(&count)
	return
}

// UpdateTime 查找表的更新时间
func (t *Table) UpdateTime() (updateTime string) {
	t.Query("SELECT `UPDATE_TIME` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").Scan(&updateTime)
	return
}

// AutoIncrement 查找表的自增ID的值
func (t *Table) AutoIncrement() (id int) {
	t.Query("SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").Scan(&id)
	return
}

// SetAutoIncrement 设置自动增长ID
func (t *Table) SetAutoIncrement(id int) error {
	_, err := t.Exec("ALTER TABLE `" + t.tableName + "` AUTO_INCREMENT = " + strconv.Itoa(id)).RowsAffected()
	return err
}

// MaxID 查找表的最大ID，如果为NULL的话则为0
func (t *Table) MaxID() (maxid int) {
	t.Query("SELECT IFNULL(MAX(id), 0) as id FROM `" + t.tableName + "`").Scan(&maxid)
	return
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
		if t.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", t.tableName, strings.Join(names, "AND")), values...).Int() > 0 {
			return 0, errInsertRepeat
		}
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

//Reads 查找
func (t *Table) Reads(m map[string]interface{}) []map[string]string {
	//SELECT * FROM address WHERE id = 1 AND uid = 27
	ks, vs := ksvs(m, " = ? ")
	return t.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RawsMap()
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
		if err == errInsertRepeat {
			return t.Update(m, keys...)
		}
		return err
	}
	return nil
}

// Delete 删除
func (t *Table) Delete(m map[string]interface{}) (int64, error) {
	ks, vs := ksvs(m, " = ? ")
	return t.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RowsAffected()
}
