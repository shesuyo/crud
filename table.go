package crud

import "strconv"
import "fmt"
import "strings"
import "errors"

type Table struct {
	*CRUD
	tableName string
}

// 返回这张表所有数据
func (t *Table) All() []map[string]string {
	return t.Query("SELECT * FROM " + t.tableName).RawsMap()
}

// 返回表有多少条数据
func (t *Table) Count() (count int) {
	t.Query("SELECT COUNT(*) FROM " + t.tableName).Scan(&count)
	return
}

// 查找表的更新时间
func (t *Table) UpdateTime() (updateTime string) {
	t.Query("SELECT `UPDATE_TIME` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").Scan(&updateTime)
	return
}

// 查找表的自增ID的值
func (t *Table) AutoIncrement() (id int) {
	t.Query("SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =(select database()) AND TABLE_NAME = '" + t.tableName + "';").Scan(&id)
	return
}

// 设置自动增长ID
func (t *Table) SetAutoIncrement(id int) error {
	return t.Exec("ALTER TABLE `" + t.tableName + "` AUTO_INCREMENT = " + strconv.Itoa(id)).err
}

// 查找表的最大ID，如果为NULL的话则为0
func (t *Table) MaxID() (maxid int) {
	t.Query("SELECT IFNULL(MAX(id), 0) as id FROM `" + t.tableName + "`").Scan(&maxid)
	return
}

/*
	创建
	check 如果有，则会判断表里面以这几个字段为唯一的话，数据库是否存在此条数据，如果有就不插入了。
*/
func (t *Table) Create(m map[string]interface{}, checks ...string) error {
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
			return errors.New("重复插入")
		}
	}
	ks, vs := ksvs(m)
	e, err := t.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", t.tableName, strings.Join(ks, ","), argslice(len(ks))), vs...).Effected()
	if err != nil {
		return errors.New("SQL语句异常")
	}
	if e <= 0 {
		return errors.New("插入数据库异常")
	}
	return nil
}
