package index

import (
	"reflect"
	"sort"
	"unsafe"
)

type PostingItem struct {
	ID        int     //doc id
	Frequency int     //词频
	Score     float64 //静态分
}

type PostingList []PostingItem

func (pl PostingList) Len() int           { return len(pl) }
func (pl PostingList) Less(i, j int) bool { return pl[i].Score > pl[j].Score } //降序
func (pl PostingList) Swap(i, j int) {
	pl[i].Score, pl[j].Score = pl[j].Score, pl[i].Score
	pl[i].ID, pl[j].ID = pl[j].ID, pl[i].ID
}
func (pl PostingList) Find(id int) *PostingItem {
	for _, item := range pl {
		if item.ID == id {
			return &item
		}
	}
	return nil
}
func (pl PostingList) IDs() []int {
	ids := make([]int, 0, len(pl))
	for _, item := range pl {
		ids = append(ids, item.ID)
	}
	sort.Sort(sort.IntSlice(ids))
	return ids
}
func (pl *PostingList) Bytes() []byte {
	var x reflect.SliceHeader
	x.Len = int(unsafe.Sizeof(*pl))
	x.Cap = x.Len
	x.Data = uintptr(unsafe.Pointer(pl))
	return *(*[]byte)(unsafe.Pointer(&x))
}
func FromBytes(buf []byte) PostingList {
	if buf == nil {
		return nil
	}
	pl := (*PostingList)(unsafe.Pointer(
		(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Data,
	))
	return *pl
}
