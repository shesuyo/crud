package crud

//Config 用于创建连接的配置配置
type Config struct {
	DataSourceName string
	MaxIdleConns   int
	MaxOpenConns   int
	Render         Render
	isRender       bool

	IsJoke bool //是否是玩笑，如果是玩笑直接返回nil,nil。用于多个项目共用同一份配置文件，但是有些数据库不需要加载。
}

func (config *Config) parse() {

}
