package crud

import (
	"testing"

	"github.com/mosesed/pluto/ekt/configuration"
)

func TestSearch_warpFieldSingel(t *testing.T) {
	member, _ := NewDataBase(configuration.MemberMySqlSourceName)
	user := member.Table("user")
	type args struct {
		field string
	}
	tests := []struct {
		name          string
		s             *Search
		args          args
		wantWarpStr   string
		wantTablename string
		wantFieldname string
	}{
		// TODO: Add test cases.
		{"1", user.Search, args{"*"}, "*", "user", "*"},
		{"1", user.Search, args{"user.*"}, "`user`.*", "user", "*"},
		{"1", user.Search, args{"user.name"}, "`user`.`name`", "user", "name"},
		{"1", user.Search, args{"user.`name`"}, "`user`.`name`", "user", "name"},
		{"1", user.Search, args{"`user`.name"}, "`user`.`name`", "user", "name"},
		{"1", user.Search, args{"`user`.`name`"}, "`user`.`name`", "user", "name"},
		{"1", user.Search, args{"name"}, "`user`.`name`", "user", "name"},
		{"1", user.Search, args{"COUNT(1)"}, "COUNT(1)", "user", "COUNT(1)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWarpStr, gotTablename, gotFieldname := tt.s.warpFieldSingel(tt.args.field)
			if gotWarpStr != tt.wantWarpStr {
				t.Errorf("Search.warpFieldSingel() gotWarpStr = %v, want %v", gotWarpStr, tt.wantWarpStr)
			}
			if gotTablename != tt.wantTablename {
				t.Errorf("Search.warpFieldSingel() gotTablename = %v, want %v", gotTablename, tt.wantTablename)
			}
			if gotFieldname != tt.wantFieldname {
				t.Errorf("Search.warpFieldSingel() gotFieldname = %v, want %v", gotFieldname, tt.wantFieldname)
			}
		})
	}
}
