package crud

import (
	"strconv"
	"testing"
)

func genRowsMap(field string, args ...int) *RowsMap {
	rm := RowsMap{}
	for _, v := range args {
		rm = append(rm, RowMap{field: strconv.Itoa(v)})
	}
	return &rm
}

func checkMap(rma, rmb RowsMap, field string) bool {
	for i := 0; i < len(rma); i++ {
		if rma[i][field] != rmb[i][field] {
			return false
		}
	}
	return true
}

func TestRowsMap_SortFunc(t *testing.T) {
	type args struct {
		f func(RowsMap, int, int) bool
	}
	tests := []struct {
		name string
		rm   *RowsMap
		args args
	}{
		{"001", genRowsMap("id", 74, 59, 238, -784, 9845, 959, 905, 0, 0, 42, 7586, -5467984, 7586), args{func(rm RowsMap, i, j int) bool {
			return rm[i].Int("id") > rm[j].Int("id")
		}}},
	}
	for _, tt := range tests {
		tt.rm.SortFunc(tt.args.f)
		for i := 0; i < len(*tt.rm)-1; i++ {
			if (*tt.rm)[i].Int("id") < (*tt.rm)[i+1].Int("id") {
				t.Fatal("rows map order wrong:", tt.rm)
			}
		}
	}
}

func TestRowsMap_MultiWarpByField(t *testing.T) {
	type args struct {
		fields []string
	}
	tests := []struct {
		name string
		rm   RowsMap
		args args
		want []MultiWarp
	}{
		// TODO: Add test cases.
		{"1", RowsMap{
			RowMap{"bid": "1", "bname": "电", "sid": "1", "sname": "灯不亮", "gid": "1", "gname": "待整理"},
			RowMap{"bid": "1", "bname": "电", "sid": "2", "sname": "灯松脱"},
			RowMap{"bid": "2", "bname": "水", "sid": "3", "sname": "没水"},
			RowMap{"bid": "2", "bname": "水", "sid": "4", "sname": "漏水"},
		}, args{fields: []string{"bid", "bname", "sid", "sname", "gid", "gname"}}, []MultiWarp{
			MultiWarp{
				ID:   "1",
				Name: "电",
				Vals: []MultiWarp{
					MultiWarp{ID: "1", Name: "灯不亮", Vals: []MultiWarp{
						MultiWarp{
							ID:   "1",
							Name: "待整理",
							Vals: []MultiWarp{},
						},
					}},
					MultiWarp{ID: "2", Name: "灯松脱", Vals: []MultiWarp{}},
				},
			},
			MultiWarp{
				ID:   "2",
				Name: "水",
				Vals: []MultiWarp{
					MultiWarp{ID: "3", Name: "没水", Vals: []MultiWarp{}},
					MultiWarp{ID: "4", Name: "漏水", Vals: []MultiWarp{}},
				},
			},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.MultiWarpByField(tt.args.fields...); stringify(got) != stringify(tt.want) {
				t.Errorf("RowsMap.MultiWarpByField() = %v, want %v", stringify(got), stringify(tt.want))
			}
		})
	}
}
