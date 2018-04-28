package crud

// Columns 用于表示一张表中的列，使用名字作为index，方便查找。
type Columns map[string]Column

// HaveColumn 是否有此列
func (cs Columns) HaveColumn(columnName string) bool {
	if cs == nil {
		return false
	}
	_, ok := cs[columnName]
	return ok
}

// Column 是描述一个具体的列
type Column struct {
	Schema     string //数据库名
	Table      string //表名
	Name       string //列名
	Comment    string //评论
	ColumnType string //列类型 tinyint(3) unsigned
	DataType   string //数据类型 tinyint
	IsNullAble bool   //是否可为NULL
}
