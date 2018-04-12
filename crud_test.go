package crud

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unsafe"
	// "github.com/jinzhu/gorm"
	// "github.com/mosesed/pluto/driver/mysql"
	// _ "github.com/go-sql-driver/mysql" //
)

// type CreateBench struct {
// 	ID   int
// 	Name string
// }

// var (
// 	dataSourceName = "root:moss7!@tcp(127.0.0.1:3306)/demo?charset=utf8"
// 	demoCRUD       = NewCRUD(dataSourceName)
// 	demoMosesed    = mysql.NewGdo()
// 	demoGORM, _    = gorm.Open("mysql", dataSourceName)
// )

// func init() {
// 	demoMosesed.Register("mysql", dataSourceName, false)
// 	demoGORM.SingularTable(true)

// }

// 任务
type Task struct {
	ID         int
	Name       string    `gorm:"size:64"` // 任务名称
	HospitalID int       //医院ID
	TemplateID int       // 模板ID
	State      int       //状态 1.发送成功 0.未发送
	CreateAt   time.Time // 创建任务时间
	StartAt    time.Time // 开始时间
	EndAt      time.Time // 结束时间
}

func TestCRUDCreate(t *testing.T) {
	//demoCRUD.Create(&CreateBench{Name: "Y"})
}

// func Benchmark_CRUDCreate(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		//Benchmark_CRUDCreate-4   	    1000	   2010405 ns/op
// 		//demoCRUD.Create(CreateBench{Name: "1"})
// 		//Benchmark_CRUDCreate-4   	    1000	   2107877 ns/op
// 		demoCRUD.Table("create_bench").Create(map[string]interface{}{"name": "1"})
// 	}
// }

//Benchmark_MosesedCreate-4   	     500	   2696521 ns/op
//func Benchmark_MosesedCreate(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		demoMosesed.NewProvider("create_bench").Insert(map[string]interface{}{"name": "1"})
//	}
//}

//Benchmark_GORMCreate-4   	    1000	   2118726 ns/op
// func Benchmark_GORMCreate(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		demoGORM.Create(&CreateBench{Name: "1"})
// 	}
// }

// var (
// 	db, _ = sql.Open("mysql", configuration.DHTPMySqlSourceName)
// )

// func init() {
// 	db.SetMaxOpenConns(100)
// }

//Benchmark_DBQuery-4      	    2000	    653021 ns/op
// func Benchmark_DBQuery(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		rows, err := db.Query("SELECT id from alias where id = 1")
// 		if err != nil {
// 			panic(err)
// 		}
// 		for rows.Next() {
// 			var id int
// 			rows.Scan(&id)
// 			continue
// 		}
// 	}
// }

//Benchmark_DBQueryRow-4   	    2000	    979942 ns/op
// func Benchmark_DBQueryRow(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		var id int
// 		row := db.QueryRow("SELECT id from alias where id = 1")
// 		err := row.Scan(&id)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }

func TestNewCRUD(t *testing.T) {
	// crud := NewCRUD("root:moss7!@/satisfaction?charset=utf8", func(w http.ResponseWriter, err error, data ...interface{}) {
	// 	m := make(map[string]interface{})
	// 	if err != nil {
	// 		m["success"] = false
	// 		m["msg"] = err.Error()
	// 	} else {
	// 		m["success"] = true
	// 		m["msg"] = true
	// 	}
	// 	if len(data) == 1 {
	// 		m["data"] = data
	// 	}
	// 	b, err := json.Marshal(m)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	w.Header().Set("Access-Control-Allow-Origin", "*")
	// 	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// 	w.Write(b)
	// })
	// fmt.Println(crud.tableNames)
	//crud.Create(&Task{})
}

// Benchmark_MapJSON-4   	  300000	      4910 ns/op	    1936 B/op	      29 allocs/op
func Benchmark_MapJSON(b *testing.B) {
	m := map[string]string{
		"a":       "asd",
		"b":       "asd",
		"e":       "asdasdasd",
		"f":       "asdasdasdqw",
		"g":       "asdsad3q4123",
		"h":       "中水电费噶多少分无人区",
		"asd":     "水电费和绿色案件人情味",
		"iemzi":   "骄傲去年初前面那阿斯顿发帖我仍股份认为镉污染",
		"asdqw12": "asd324236gsdf456152e1sda5f1dsa5f1sda5f15sdaf1asd",
		"happy":   "to be or not to be ,that's a question!",
	}
	for i := 0; i < b.N; i++ {
		bs, _ := json.Marshal(m)
		_ = bs
	}
}

func Benchmark_MapJSONSelf(b *testing.B) {
	m := map[string]string{
		"a":       "asd",
		"b":       "asd",
		"e":       "asdasdasd",
		"f":       "asdasdasdqw",
		"g":       "asdsad3q4123",
		"h":       "中水电费噶多少分无人区",
		"asd":     "水电费和绿色案件人情味",
		"iemzi":   "骄傲去年初前面那阿斯顿发帖我仍股份认为镉污染",
		"asdqw12": "asd324236gsdf456152e1sda5f1dsa5f1sda5f15sdaf1asd",
		"happy":   "to be or not to be ,that's a question!",
	}
	for i := 0; i < b.N; i++ {
		bs := mapMarshal(m)
		_ = bs
	}
}

func byteString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func stringByte(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// // convert b to string without copy
// func BytesString(b []byte) String {
// 	return *(*String)(unsafe.Pointer(&b))
// }

func mapMarshal(m map[string]string) []byte {
	sb := strings.Builder{}
	sb.WriteString("{")
	l := len(m)
	n := 0
	for k, v := range m {
		sb.WriteString(`"` + k + `":"` + v + `"`)
		n++
		if n < l {
			sb.WriteString(",")
		}
	}
	sb.WriteString("}")
	return stringByte(sb.String())
}
