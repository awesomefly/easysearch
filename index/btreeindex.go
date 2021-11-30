//
// btree inverted index's data structure
//
// |<-------------btree------------>| <--posting list--> |
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
//| <--in memory--> | <-----------on disk--------------->|
//
//

package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"unsafe"

	btree "github.com/awesomefly/gobtree"
	"github.com/awesomefly/simplefts/common"
	"github.com/awesomefly/simplefts/store"
)

var DefaultConfig = btree.Config{
	//Idxfile: "/Users/bytedance/go/src/github.com/simplefts/data/test_insread_index.dat",
	//Kvfile:  "/Users/bytedance/go/src/github.com/simplefts/data/test_insread_kv.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      4,
	RebalanceThrs: 30,
	AppendRatio:   0.7,
	DrainRate:     200,
	MaxLeafCache:  0, // intermediate node in memory and leaf node in disk
	Sync:          false,
	Nocache:       false,
}

type BTreeIndex struct {
	BT *btree.BTree

	// DocNum is the count of documents
	DocNum int

	// Len is the total length of docs
	Len int

	Options
}

func NewBTreeIndex(file string) *BTreeIndex {
	conf := DefaultConfig
	conf.Idxfile, conf.Kvfile = file+".idx", file+".kv"
	bt := BTreeIndex{
		BT: btree.NewBTree(btree.NewStore(conf)), // todo: 索引文件太大
	}

	bt.Options.StoreFile = file + ".sum"
	bt.Load()
	return &bt
}

func (bt *BTreeIndex) Save() {
	os.Create(bt.Options.StoreFile)

	// Index store
	fd, err := os.OpenFile(bt.Options.StoreFile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	buffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.DocNum)); err != nil {
		panic(err)
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(bt.Len)); err != nil {
		panic(err)
	}

	if _, err := fd.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
	fd.Close()
}

func (bt *BTreeIndex) Load() {
	// Index store
	fd, err := os.OpenFile(bt.Options.StoreFile, os.O_RDONLY|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	data := make([]byte, unsafe.Sizeof(bt.DocNum)+unsafe.Sizeof(bt.Len))
	if n, err := fd.Read(data); err != nil {
		if n == 0 && err == io.EOF {
			return
		}
		panic(err.Error())
	}

	buffer := bytes.NewBuffer(data)
	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.DocNum))); err != nil {
		panic(err.Error())
	}

	if err := binary.Read(buffer, binary.LittleEndian, (*int32)(unsafe.Pointer(&bt.Len))); err != nil {
		panic(err.Error())
	}

	fd.Close()
}

func (bt *BTreeIndex) Close() {
	bt.BT.Drain()
	bt.BT.Close()
	bt.Save()
}

func (bt *BTreeIndex) Lookup(token string, dirty bool) PostingList {
	key := &btree.TestKey{K: token}

	var ch chan []byte
	if dirty {
		ch = bt.BT.LookupDirty(key)
	} else {
		ch = bt.BT.Lookup(key) //todo: btree内部节点key值直接查文件，用kdping缓存加速查询
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

func (bt *BTreeIndex) Add(docs []store.Document) {
	for _, doc := range docs {
		tokens := common.Analyze(doc.Text)
		for _, token := range tokens {
			key := &btree.TestKey{K: token}
			postingList := bt.Lookup(token, true)
			if postingList != nil {
				if last := &postingList[len(postingList)-1]; last.ID == int32(doc.ID) {
					// Don't add same ID twice. But should update frequency
					last.TF++
					last.Score = CalDocScore(last.TF, 0)
					bt.BT.Insert(key, postingList)
					continue
				}
			}
			item := Doc{
				ID:     int32(doc.ID),
				DocLen: int32(len(tokens)),
				TF:     1,
				Score:  CalDocScore(1, 0),
			}
			//add to posting list
			postingList = append(postingList, item)
			bt.BT.Insert(key, postingList)
		}
		bt.DocNum++
		bt.Len += len(tokens) // todo: adding only unique words
	}
	bt.BT.Drain()

	//sort by score
	ch := bt.BT.FullSet()
	for {
		k := <-ch
		d := <-ch
		v := <-ch
		if k == nil || d == nil || v == nil {
			break
		}

		var nv PostingList
		nv.FromBytes(v)
		sort.Slice(nv, func(i, j int) bool {
			return nv[i].Score > nv[j].Score
		})

		id, err := strconv.ParseInt(string(d), 10, 64) //TestKey.Docid()对应
		if err != nil {
			panic(err)
		}

		key := &btree.TestKey{K: string(k), Id: id}
		bt.BT.Insert(key, nv)
	}
	bt.BT.Drain()
}

// Retrieval 布尔检索，bm25计算相关性
func (bt *BTreeIndex) Retrieval(must []string, should []string, not []string, k int, r int) []Doc {
	table := make(map[KeyWord][]Doc, 0)

	var result PostingList
	for _, term := range must {
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按frequency排序,截断前r个,加速归并
			sort.Sort(plr)
			if result == nil {
				result = plr
			} else {
				result.Inter(plr)
			}
			keyword := KeyWord{K: term, DF: int32(len(pl))}
			table[keyword] = plr
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		if pl := bt.Lookup(term, false); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}
			keyword := KeyWord{K: term, DF: int32(len(pl))}
			table[keyword] = plr
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl := bt.Lookup(term, false); pl != nil {
			sort.Sort(pl)
			result.Filter(pl)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	// 计算bm25 参考:https://www.jianshu.com/p/1e498888f505
	for i, hit := range result {
		for keyword, docs := range table {
			doc := (PostingList)(docs).Find(int(hit.ID))
			if doc == nil {
				continue
			}

			d := float64(doc.DocLen)
			avg := float64(bt.Len) / float64(bt.DocNum)
			idf := math.Log2(float64(bt.DocNum)/float64(keyword.DF) + 1)
			k1 := float64(2)
			b := 0.75
			result[i].BM25 += idf * float64(doc.TF) * (k1 + 1) / (float64(doc.TF) + k1*(1-b+b*d/avg))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].BM25 > result[j].BM25 //降序
	})

	if len(result) > k {
		return result[:k]
	}
	return result
}
