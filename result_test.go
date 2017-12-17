package crud

import "testing"
import "strconv"

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
