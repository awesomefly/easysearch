package index

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/xtgo/set"
)

type Term struct {
	K  string //key
	Id int32  //key id
	DF int32  //Document Frequency
}

type Doc struct {
	ID           int32   //doc id
	DocLen       int32   //doc length

	TF     int32   //词频, eg. 在倒排表term->[doc1,doc2,doc3]中，仅表示term在docX中的词频
	QualityScore float64 //静态分、质量分

	Score  float64 //bm25/Cosine score used by sort
}

func (doc Doc) Bytes() []byte {
	buffer := bytes.NewBuffer([]byte{})
	err := binary.Write(buffer, binary.LittleEndian, doc)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func (doc *Doc) FromBytes(b []byte) {
	buffer := bytes.NewBuffer(b)

	err := binary.Read(buffer, binary.LittleEndian, doc)
	if err != nil {
		panic(err)
	}
}

type PostingList []Doc

func (pl PostingList) Len() int           { return len(pl) }
func (pl PostingList) Less(i, j int) bool { return pl[i].ID > pl[j].ID } //降序, sort by score
func (pl PostingList) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

func (pl PostingList) Find(id int) *Doc {
	for i := 0; i < pl.Len(); i++ {
		if pl[i].ID == int32(id) {
			return &pl[i]
		}
	}
	return nil
}

func (pl PostingList) IDs() []int {
	ids := make([]int, 0, len(pl))
	for _, item := range pl {
		ids = append(ids, int(item.ID))
	}
	sort.Sort(sort.IntSlice(ids))
	return ids
}

func (pl *PostingList) Inter(docs []Doc) {
	l := len(*pl)
	*pl = append(*pl, docs...)
	size := set.Inter(pl, l)
	*pl = (*pl)[:size]
}

func (pl *PostingList) Union(docs []Doc) {
	l := len(*pl)
	*pl = append(*pl, docs...)
	size := set.Union(pl, l)
	*pl = (*pl)[:size]
}

func (pl *PostingList) Filter(docs []Doc) {
	l := len(*pl)
	join := append(*pl, docs...)
	size := set.Inter(join, l)
	inter := join[:size]

	*pl = append(*pl, inter...)
	size = set.Diff(pl, l)
	*pl = (*pl)[:size]
}

func (pl *PostingList) Append(docs ...Doc) {
	*pl = append(*pl, docs...)
}

func (pl PostingList) Bytes() []byte {
	buffer := bytes.NewBuffer([]byte{})
	for _, v := range pl {
		err := binary.Write(buffer, binary.LittleEndian, v)
		if err != nil {
			panic(err)
		}
	}
	return buffer.Bytes()
}

func (pl *PostingList) FromBytes(buf []byte) {
	if buf == nil {
		return
	}

	buffer := bytes.NewBuffer(buf)
	for buffer.Len() > 0 {
		var item Doc
		binary.Read(buffer, binary.LittleEndian, &item)
		*pl = append(*pl, item)
	}
}
