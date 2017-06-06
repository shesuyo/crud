package crud

import (
	"database/sql"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" //底层使用，目前只支持mysql数据库。
)

//并发默认100

type DB struct {
	*sql.DB
	isIdle bool
}

func (db *DB) Close() {
	db.isIdle = true
}

type Pools struct {
	dsn string
	max int
	mu  sync.RWMutex
	dbs []*DB
}

func (p *Pools) Open() (*DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := len(p.dbs)
	for {
		for i := 0; i < count; i++ {
			if p.dbs[i].isIdle {
				p.dbs[i].isIdle = false
				return p.dbs[i], nil
			}
		}

		if p.max > count {
			db, err := sql.Open("mysql", p.dsn)
			if err != nil {
				log.Println(err)
			}
			warp := &DB{DB: db, isIdle: false}
			p.dbs = append(p.dbs, warp)
			return warp, err
		}
		time.Sleep(5e4)
	}

	return nil, nil
}

func NewPool(dsn string, maxConnec int) *Pools {
	return &Pools{dsn: dsn, max: maxConnec}
}
