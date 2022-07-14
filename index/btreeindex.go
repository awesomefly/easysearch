//
// btree inverted index's data structure
//
// |<-------------btree------------>| <--posting list-->|
// |<-intermediate->|<--leaf node-->|
//				 -     --- ---          --- --- --- ---
//				| | - |   |   |    -   |   |   |   |   |
//               -     --- ---          --- --- --- ---
//			  /
//		   -     -     --- ---          --- --- --- ---
//        | | - | | - |   |   |    -   |   |   |   |   |
//		   -     -	   --- ---          --- --- --- ---
//		/
//   -     -     -     --- ---          --- --- --- ---
//	| | - | | - | | - |   |   |    -   |   |   |   |   |
//	 -	   -     -     --- ---          --- --- --- ---
//		\
//		   -     -     --- ---          --- --- --- ---
//        | | - | | - |   |   |    -   |   |   |   |   |
//		   -	 -     --- ---          --- --- --- ---
// |<--in memory--> | <-----------on disk-------------->|
//
//

package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/awesomefly/easysearch/util"
	btree "github.com/awesomefly/gobtree"
)

var DefaultConfig = btree.Config{
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      4,
	RebalanceThrs: 30,
	AppendRatio:   0.7,
	DrainRate:     100,
	MaxLeafCache:  0, // intermediate node in memory and leaf node in disk
	Sync:          false,
	Nocache:       false,
}

type BTreeIndex struct {
	//skip-list vs btree:
	//https://stackoverflow.com/questions/256511/skip-list-vs-binary-search-tree/28270537#28270537
	BT        *btree.BTree
	IndexFile string

	property Property
}

func NewBTreeIndex(file string) *BTreeIndex {
	conf := DefaultConfig
	conf.Idxfile, conf.Kvfile = file+".idx", file+".kv"
	bt := BTreeIndex{
		IndexFile: file,
		BT:        btree.NewBTree(btree.NewStore(conf)), // todo: 索引文件太大，索引压缩、posting list压缩
		property: Property{
			docNum:     0,
			tokenCount: 0,
			dataRange: DataRange{Start: 0, End: 0},
		},
	}

	bt.Load()
	return &bt
}

func (bt *BTreeIndex) Save() {
	file := bt.IndexFile + ".sum"
	os.Create(file)

	// Index store
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	buffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.property.docNum)); err != nil {
		panic(err)
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.property.tokenCount)); err != nil {
		panic(err)
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.property.dataRange.Start)); err != nil {
		panic(err)
	}
	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.property.dataRange.End)); err != nil {
		panic(err)
	}

	if _, err := fd.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
	fd.Close()
}

func (bt *BTreeIndex) Load() {
	// Index store
	file := bt.IndexFile + ".sum"
	fd, err := os.OpenFile(file, os.O_RDONLY|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	data := make([]byte, unsafe.Sizeof(bt.property.docNum)+unsafe.Sizeof(bt.property.tokenCount))
	if n, err := fd.Read(data); err != nil {
		if n == 0 && err == io.EOF {
			return
		}
		panic(err.Error())
	}

	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.property.docNum))); err != nil {
		panic(err.Error())
	}

	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.property.tokenCount))); err != nil {
		panic(err.Error())
	}
	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.property.dataRange.Start))); err != nil {
		panic(err.Error())
	}
	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.property.dataRange.End))); err != nil {
		panic(err.Error())
	}

	fd.Close()
}

func (bt *BTreeIndex) Close() {
	bt.BT.Drain()
	bt.BT.Close()
	bt.Save()
}

func (bt *BTreeIndex) Clear() {
	bt.Close()

	// delete deprecated index
	os.Remove(bt.IndexFile + ".sum")
	os.Remove(bt.IndexFile + ".idx")
	os.Remove(bt.IndexFile + ".kv")
}

func (bt *BTreeIndex) Keys() []string {
	keys := make(sort.StringSlice, bt.Property().tokenCount)

	ch := bt.BT.KeySet()
	for {
		key := <-ch
		if key == nil {
			break
		}
		keys = append(keys, string(key))
	}
	return keys
}

func (bt *BTreeIndex) Lookup(token string, dirty bool) PostingList {
	key := &btree.TestKey{K: token}

	var ch chan []byte
	if dirty {
		ch = bt.BT.LookupDirty(key)
	} else {
		ch = bt.BT.Lookup(key)
	}
	values := make([][]byte, 0)
	for {
		x := <-ch
		if x == nil {
			break
		}
		values = append(values, x)
	}

	if len(values) == 0 {
		return nil
	}

	var p PostingList
	p.FromBytes(values[0])
	return p
}

// Add 该方法比较低效，批量插入文档会在posting list后不段追加新文档，但postinglist并未预留空间，
// 因此需要移动到新的空间，导致文件数据拷贝
func (bt *BTreeIndex) Add(docs []Document) {
	for _, doc := range docs {
		tokens := util.Analyze(doc.Text)
		for _, token := range tokens {
			//log.Printf("token:%s", token)
			key := &btree.TestKey{K: token}
			postingList := bt.Lookup(token, true)
			if postingList != nil {
				if last := postingList.Find(doc.ID); last != nil {
					// Don't add same ID twice. But should update frequency
					last.TF++
					last.QualityScore = CalDocScore(last.TF, 0)
					bt.BT.Insert(key, postingList)
					continue
				}
			}
			item := Doc{
				ID:           int32(doc.ID),
				DocLen:       int32(len(tokens)),
				TF:           1,
				QualityScore: CalDocScore(1, 0),
			}
			//add to posting list & sort by score
			postingList = append(postingList, item)
			sort.Slice(postingList, func(i, j int) bool {
				return postingList[i].QualityScore > postingList[j].QualityScore
			})
			bt.BT.Insert(key, postingList)
		}
		bt.property.docNum++
		bt.property.tokenCount += len(tokens)
	}
	bt.BT.Drain()
}

func (bt *BTreeIndex) Insert(key string, pl PostingList) {
	bt.BT.Insert(&btree.TestKey{K: key}, pl)
	bt.property.docNum += pl.Len()
	bt.property.tokenCount++
}

func (bt *BTreeIndex) Get(term string) []Doc {
	if postingList := bt.Lookup(term, false); postingList != nil {
		return postingList
	}
	return nil
}

func (bt *BTreeIndex) Property() *Property {
	return &bt.property
}

func (bt *BTreeIndex) SetProperty(p Property) {
	bt.property = p
}

func (bt *BTreeIndex) Retrieval(must []string, should []string, not []string, k int, r int, m SearchModel) []Doc {
	return DoRetrieval(bt, must, should, not, k, r, m)
}