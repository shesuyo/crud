package crud

import "strconv"

type Table struct {
	*CRUD
	tableName string
}

// 返回这张表所有数据
func (t *Table) All() []map[string]string {
	return t.Query("SELECT * FROM " + t.tableName).RawsMap()
}

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
