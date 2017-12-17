package crud

import "strconv"

// CIDFields cid可能的字段
var CIDFields = [...]string{"categoryid", "cid", "hospital_id"}

// UIDFields uid可能的字段
var UIDFields = [...]string{"uid"}

// NameFields name可能的字段
var NameFields = [...]string{"name", "nickname", "title"}

// CID return cid
func (rm RowMap) CID() string {
	var cid string
	var ok bool
	for _, field := range CIDFields {
		if cid, ok = rm[field]; ok {
			return cid
		}
	}
	return cid
}

// ID return id
func (rm RowMap) ID() string {
	return rm["id"]
}

// IDInt return id type int
func (rm RowMap) IDInt() int {
	return rm.Int("id")
}

// CIDInt return cid int值
func (rm RowMap) CIDInt() int {
	cid, _ := strconv.Atoi(rm.CID())
	return cid
}

// Name return name type string
func (rm RowMap) Name() string {
	var name string
	var ok bool
	for _, field := range NameFields {
		if name, ok = rm[field]; ok {
			return name
		}
	}
	return name
}

// UID return uid type string
func (rm RowMap) UID() string {
	var uid string
	var ok bool
	for _, field := range UIDFields {
		if uid, ok = rm[field]; ok {
			return uid
		}
	}
	return uid
}

// UIDInt return uid type int
func (rm RowMap) UIDInt() int {
	uid, _ := strconv.Atoi(rm.UID())
	return uid
}
