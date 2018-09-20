package crud

// Config 用于创建连接的配置配置
type Config struct {
	DataSourceName string
	MaxIdleConns   int
	MaxOpenConns   int
	Render         Render
	isRender       bool
}

func (config *Config) parse() {

}
