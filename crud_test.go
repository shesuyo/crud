package crud

import (
	"testing"
	"time"
)

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
